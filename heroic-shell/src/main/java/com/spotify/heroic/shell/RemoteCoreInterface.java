/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.shell;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.google.protobuf.ByteString;
import com.spotify.heroic.proto.ShellMessage.CommandEvent;
import com.spotify.heroic.proto.ShellMessage.CommandsResponse;
import com.spotify.heroic.proto.ShellMessage.FileEvent;
import com.spotify.heroic.proto.ShellMessage.FileRead;
import com.spotify.heroic.proto.ShellMessage.FileStream;
import com.spotify.heroic.proto.ShellMessage.FileWrite;
import com.spotify.heroic.proto.ShellMessage.Message;
import com.spotify.heroic.shell.protocol.MessageBuilder;
import com.spotify.heroic.shell.protocol.SimpleMessageVisitor;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteCoreInterface implements CoreInterface {
    private static final int DEFAULT_PORT = 9190;
    private static final int MAX_READ = 4096;


    private final InetSocketAddress address;
    private final AsyncFramework async;

    public RemoteCoreInterface(InetSocketAddress address, AsyncFramework async) {
        this.address = address;
        this.async = async;
    }

    @Override
    public AsyncFuture<Void> evaluate(final List<String> commands, final ShellIO io) {
        return async.call(() -> {
            final AtomicBoolean running = new AtomicBoolean(true);
            final AtomicInteger fileCounter = new AtomicInteger();

            final Map<Integer, InputStream> reading = new HashMap<>();
            final Map<Integer, OutputStream> writing = new HashMap<>();
            final Map<Integer, Callable<Void>> closers = new HashMap<>();

            try (final ShellConnection c = connect()) {
                c.send(MessageBuilder.evaluateRequest(commands));

                final SimpleMessageVisitor<Optional<Message>> visitor =
                    setupVisitor(io, running, fileCounter, reading, writing, closers);

                while (true) {
                    final Optional<Message> out = c.receiveAndParse(visitor);

                    if (!running.get()) {
                        break;
                    }

                    if (out.isPresent()) {
                        final Message o = out.get();

                        try {
                            c.send(o);
                        } catch (Exception e) {
                            throw new Exception("Failed to send response: " + o, e);
                        }
                    }
                }
            }

            return null;
        });
    }

    private SimpleMessageVisitor<Optional<Message>> setupVisitor(
        final ShellIO io, final AtomicBoolean running, final AtomicInteger fileCounter,
        final Map<Integer, InputStream> reading, final Map<Integer, OutputStream> writing,
        final Map<Integer, Callable<Void>> closers
    ) {
        return new SimpleMessageVisitor<Optional<Message>>() {
            public Optional<Message> visitCommandDone(CommandEvent msg) {
                running.set(false);
                return empty();
            }

            @Override
            public Optional<Message> visitCommandOutput(CommandEvent msg) {
                io.out().write(msg.getData());
                return empty();
            }

            @Override
            public Optional<Message> visitCommandOutputFlush(CommandEvent msg) {
                io.out().flush();
                return empty();
            }

            @Override
            public Optional<Message> visitFileNewInputStream(FileStream msg) throws Exception {
                final InputStream in = io.newInputStream(Paths.get(msg.getPath()));
                final int handle = fileCounter.incrementAndGet();

                reading.put(handle, in);
                closers.put(handle, () -> {
                    in.close();
                    reading.remove(handle);
                    return null;
                });

                return of(MessageBuilder.fileEvent(handle, FileEvent.Event.OPENED));
            }

            @Override
            public Optional<Message> visitFileNewOutputStream(FileStream msg) throws Exception {
                final OutputStream out = io.newOutputStream(Paths.get(msg.getPath()));
                final int handle = fileCounter.incrementAndGet();

                writing.put(handle, out);
                closers.put(handle, () -> {
                    out.close();
                    writing.remove(handle);
                    return null;
                });

                return of(MessageBuilder.fileEvent(handle, FileEvent.Event.OPENED));
            }

            @Override
            public Optional<Message> visitFileFlush(FileEvent msg) throws Exception {
                writer(msg.getHandle()).flush();
                return of(MessageBuilder.acknowledge());
            }

            @Override
            public Optional<Message> visitFileClose(FileEvent msg) throws Exception {
                closer(msg.getHandle()).call();
                return of(MessageBuilder.acknowledge());
            }

            @Override
            public Optional<Message> visitFileWrite(FileWrite msg) throws Exception {
                final byte[] data = msg.getDataBytes().toByteArray();
                writer(msg.getHandle()).write(data, 0, data.length);
                return empty();
            }

            @Override
            public Optional<Message> visitFileRead(FileRead msg) throws Exception {
                final byte[] buffer = new byte[Math.min(MAX_READ, msg.getLength())];

                int read = reader(msg.getHandle()).read(buffer);

                final ByteString bytes;
                if (read == 0) {
                    bytes = ByteString.EMPTY;
                } else {
                    bytes = ByteString.copyFrom(buffer);
                }

                return of(MessageBuilder.fileReadResult(bytes));
            }

            private InputStream reader(int handle) throws Exception {
                final InputStream r = reading.get(handle);

                if (r == null) {
                    throw new Exception("No such handle: " + handle);
                }

                return r;
            }

            private OutputStream writer(int handle) throws Exception {
                final OutputStream w = writing.get(handle);

                if (w == null) {
                    throw new Exception("No such handle: " + handle);
                }

                return w;
            }

            private Callable<Void> closer(int handle) throws Exception {
                final Callable<Void> closer = closers.get(handle);

                if (closer == null) {
                    throw new Exception("No such handle: " + handle);
                }

                return closer;
            }
        };
    }

    @Override
    public List<CommandsResponse.CommandDefinition> commands() throws IOException {
        try (final ShellConnection c = connect()) {
            final Message message = MessageBuilder.commandEvent(CommandEvent.Event.REQUEST);
            return c.request(message).getCommandsResponse().getCommandsList();
        }
    }

    public static RemoteCoreInterface fromConnectString(String connect, AsyncFramework async)  {
        final String host;
        final int port;

        final int index;

        if ((index = connect.indexOf(':')) > 0) {
            host = connect.substring(0, index);
            port = Integer.parseInt(connect.substring(index + 1));
        } else {
            host = connect;
            port = DEFAULT_PORT;
        }

        return new RemoteCoreInterface(new InetSocketAddress(host, port), async);
    }

    private ShellConnection connect() throws IOException {
        final Socket socket = new Socket();

        socket.connect(address);

        return new ShellConnection(socket);
    }
}
