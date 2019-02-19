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

import com.spotify.heroic.ShellTasks;
import com.spotify.heroic.proto.ShellMessage.CommandEvent;
import com.spotify.heroic.proto.ShellMessage.CommandEvent.Event;
import com.spotify.heroic.proto.ShellMessage.EvaluateRequest;
import com.spotify.heroic.shell.protocol.MessageBuilder;
import com.spotify.heroic.shell.protocol.SimpleMessageVisitor;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.SerializerFramework;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ShellServerClientThread implements Runnable {
    final Socket socket;
    final ShellTasks tasks;
    final SerializerFramework serializer;
    final AsyncFramework async;

    @java.beans.ConstructorProperties({ "socket", "tasks", "serializer", "async" })
    public ShellServerClientThread(final Socket socket, final ShellTasks tasks,
                                   final SerializerFramework serializer,
                                   final AsyncFramework async) {
        this.socket = socket;
        this.tasks = tasks;
        this.serializer = serializer;
        this.async = async;
    }

    @Override
    public void run() {
        try {
            doRun();
        } catch (Exception e) {
            log.error("Exception thrown in client thread", e);
        }

        try {
            socket.close();
        } catch (IOException e) {
            log.error("Failed to close client socket", e);
        }
    }

    void doRun() throws Exception {
        try (final ServerConnection ch = new ServerConnection(socket)) {
            try (final PrintWriter out = setupPrintWriter(ch)) {
                final ShellIO io = setupShellIO(ch, out);

                final SimpleMessageVisitor<AsyncFuture<Void>> visitor =
                    new SimpleMessageVisitor<AsyncFuture<Void>>() {
                        @Override
                        public AsyncFuture<Void> visitCommandsRequest(CommandEvent message)
                            throws Exception {
                            ch.send(MessageBuilder.commandsResponse(tasks.commands()));
                            return async.resolved();
                        }

                        @Override
                        public AsyncFuture<Void> visitRunTaskRequest(EvaluateRequest message)
                            throws Exception {
                            log.info("Run task: {}", message);

                            return tasks.evaluate(message.getCommandList(), io);
                        }

                        @Override
                        public AsyncFuture<Void> visitUnknown() {
                            return async.failed(new RuntimeException("Unhandled message"));
                        }
                    };

                final AsyncFuture<Void> future = ch.receiveAndParse(visitor);

                try {
                    future.get();
                } catch (Exception e) {
                    log.error("Command Failed", e);
                    out.println("Command Failed: " + e.getMessage());
                    e.printStackTrace(out);
                }
            }

            ch.send(MessageBuilder.commandEvent(Event.DONE));
        }
    }

    private ShellIO setupShellIO(final ServerConnection ch, final PrintWriter out) {
        return new ShellIO() {
            @Override
            public PrintWriter out() {
                return out;
            }

            @Override
            public OutputStream newOutputStream(Path path) throws IOException {
                return ch.newOutputStream(path);
            }

            @Override
            public InputStream newInputStream(Path path) throws IOException {
                return ch.newInputStream(path);
            }
        };
    }

    private PrintWriter setupPrintWriter(final ServerConnection ch) {
        return new PrintWriter(new Writer() {
            @Override
            public void close() throws IOException {
                flush();
            }

            @Override
            public void flush() throws IOException {
                ch.send(MessageBuilder.commandEvent(Event.DONE));
            }

            @Override
            public void write(char[] b, int off, int len) throws IOException {
                final char[] out = Arrays.copyOfRange(b, off, len);
                ch.send(MessageBuilder.commandEvent(Event.OUTPUT, new String(out)));
            }
        }, true);
    }
}
