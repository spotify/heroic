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

import com.spotify.heroic.shell.protocol.Acknowledge;
import com.spotify.heroic.shell.protocol.CommandDefinition;
import com.spotify.heroic.shell.protocol.CommandDone;
import com.spotify.heroic.shell.protocol.CommandOutput;
import com.spotify.heroic.shell.protocol.CommandsRequest;
import com.spotify.heroic.shell.protocol.CommandsResponse;
import com.spotify.heroic.shell.protocol.EvaluateRequest;
import com.spotify.heroic.shell.protocol.FileClose;
import com.spotify.heroic.shell.protocol.FileFlush;
import com.spotify.heroic.shell.protocol.FileNewInputStream;
import com.spotify.heroic.shell.protocol.FileNewOutputStream;
import com.spotify.heroic.shell.protocol.FileOpened;
import com.spotify.heroic.shell.protocol.FileRead;
import com.spotify.heroic.shell.protocol.FileReadResult;
import com.spotify.heroic.shell.protocol.FileWrite;
import com.spotify.heroic.shell.protocol.Message;
import com.spotify.heroic.shell.protocol.SimpleMessageVisitor;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.SerializerFramework;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoteCoreInterface implements CoreInterface {
    public static final int DEFAULT_PORT = 9190;

    final InetSocketAddress address;
    final AsyncFramework async;
    final SerializerFramework serializer;

    public RemoteCoreInterface(InetSocketAddress address, AsyncFramework async,
            SerializerFramework serializer) throws IOException {
        this.address = address;
        this.async = async;
        this.serializer = serializer;
    }

    @Override
    public AsyncFuture<Void> evaluate(final List<String> command, final ShellIO io)
            throws Exception {
        return async.call(() -> {
            final AtomicBoolean running = new AtomicBoolean(true);
            final AtomicInteger fileCounter = new AtomicInteger();

            final Map<Integer, InputStream> reading = new HashMap<>();
            final Map<Integer, OutputStream> writing = new HashMap<>();
            final Map<Integer, Callable<Void>> closers = new HashMap<>();

            try (final ShellConnection c = connect()) {
                c.send(new EvaluateRequest(command));

                final Message.Visitor<Optional<Message>> visitor =
                        setupVisitor(io, running, fileCounter, reading, writing, closers);

                while (true) {
                    final Message in = c.receive();

                    final Optional<Message> out = in.visit(visitor);

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

    private SimpleMessageVisitor<Optional<Message>> setupVisitor(final ShellIO io,
            final AtomicBoolean running, final AtomicInteger fileCounter,
            final Map<Integer, InputStream> reading, final Map<Integer, OutputStream> writing,
            final Map<Integer, Callable<Void>> closers) {
        return new SimpleMessageVisitor<Optional<Message>>() {
            public Optional<Message> visitCommandDone(CommandDone m) {
                running.set(false);
                return empty();
            }

            @Override
            public Optional<Message> visitCommandOutput(CommandOutput m) {
                io.out().write(m.getData());
                io.out().flush();
                return empty();
            }

            @Override
            public Optional<Message> visitFileNewInputStream(FileNewInputStream m)
                    throws Exception {
                final InputStream in =
                        io.newInputStream(Paths.get(m.getPath()), m.getOptionsAsArray());

                final int h = fileCounter.incrementAndGet();

                reading.put(h, in);
                closers.put(h, new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        in.close();
                        reading.remove(h);
                        return null;
                    }
                });

                return of(new FileOpened(h));
            }

            @Override
            public Optional<Message> visitFileNewOutputStream(FileNewOutputStream m)
                    throws Exception {
                final OutputStream out =
                        io.newOutputStream(Paths.get(m.getPath()), m.getOptionsAsArray());

                final int h = fileCounter.incrementAndGet();

                writing.put(h, out);
                closers.put(h, new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        out.close();
                        writing.remove(h);
                        return null;
                    }
                });

                return of(new FileOpened(h));
            }

            @Override
            public Optional<Message> visitFileFlush(FileFlush m) throws Exception {
                writer(m.getHandle()).flush();
                return of(new Acknowledge());
            }

            @Override
            public Optional<Message> visitFileClose(FileClose m) throws Exception {
                closer(m.getHandle()).call();
                return of(new Acknowledge());
            }

            @Override
            public Optional<Message> visitFileWrite(FileWrite m) throws Exception {
                final byte[] data = m.getData();
                writer(m.getHandle()).write(data, 0, data.length);
                return of(new Acknowledge());
            }

            @Override
            public Optional<Message> visitFileRead(FileRead m) throws Exception {
                final byte[] buffer = new byte[m.getLength()];
                reader(m.getHandle()).read(buffer, 0, m.getLength());
                return of(new FileReadResult(buffer));
            }

            @Override
            protected Optional<Message> visitUnknown(Message message) {
                throw new IllegalArgumentException("Unhandled message: " + message);
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
    public List<CommandDefinition> commands() throws Exception {
        try (final ShellConnection c = connect()) {
            return c.request(new CommandsRequest(), CommandsResponse.class).getCommands();
        }
    }

    @Override
    public void shutdown() throws Exception {
    }

    public static RemoteCoreInterface fromConnectString(String connect, AsyncFramework async,
            SerializerFramework serializer) throws IOException {
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

        return new RemoteCoreInterface(new InetSocketAddress(host, port), async, serializer);
    }

    private ShellConnection connect() throws IOException {
        final Socket socket = new Socket();

        socket.connect(address);

        return new ShellConnection(serializer, socket);
    }
}
