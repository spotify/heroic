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
import com.spotify.heroic.shell.protocol.CommandDone;
import com.spotify.heroic.shell.protocol.CommandOutput;
import com.spotify.heroic.shell.protocol.CommandsRequest;
import com.spotify.heroic.shell.protocol.CommandsResponse;
import com.spotify.heroic.shell.protocol.EvaluateRequest;
import com.spotify.heroic.shell.protocol.Message;
import com.spotify.heroic.shell.protocol.SimpleMessageVisitor;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.SerializerFramework;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@Slf4j
@RequiredArgsConstructor
class ShellServerClientThread implements Runnable {
    final Socket socket;
    final ShellTasks tasks;
    final SerializerFramework serializer;
    final AsyncFramework async;

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
        try (final ServerConnection ch = new ServerConnection(serializer, socket)) {
            try (final PrintWriter out = setupPrintWriter(ch)) {
                final ShellIO io = setupShellIO(ch, out);

                final SimpleMessageVisitor<AsyncFuture<Void>> visitor =
                    new SimpleMessageVisitor<AsyncFuture<Void>>() {
                        @Override
                        public AsyncFuture<Void> visitCommandsRequest(CommandsRequest message)
                            throws Exception {
                            ch.send(new CommandsResponse(tasks.commands()));
                            return async.resolved();
                        }

                        @Override
                        public AsyncFuture<Void> visitRunTaskRequest(EvaluateRequest message)
                            throws Exception {
                            log.info("Run task: {}", message);

                            return tasks.evaluate(message.getCommand(), io);
                        }

                        @Override
                        protected AsyncFuture<Void> visitUnknown(Message message) throws Exception {
                            return async.failed(
                                new RuntimeException("Unhandled message: " + message));
                        }
                    };

                final AsyncFuture<Void> future = ch.receive().visit(visitor);

                try {
                    future.get();
                } catch (Exception e) {
                    log.error("Command Failed", e);
                    out.println("Command Failed: " + e.getMessage());
                    e.printStackTrace(out);
                }
            }

            ch.send(new CommandDone());
        }
    }

    private ShellIO setupShellIO(final ServerConnection ch, final PrintWriter out) {
        return new ShellIO() {
            @Override
            public PrintWriter out() {
                return out;
            }

            @Override
            public OutputStream newOutputStream(Path path, StandardOpenOption... options)
                throws IOException {
                return ch.newOutputStream(path, options);
            }

            @Override
            public InputStream newInputStream(Path path, StandardOpenOption... options)
                throws IOException {
                return ch.newInputStream(path, options);
            }
        };
    }

    private PrintWriter setupPrintWriter(final ServerConnection ch) {
        return new PrintWriter(new Writer() {
            private CharArrayWriter out = new CharArrayWriter(ServerConnection.BUFFER_SIZE);

            @Override
            public void close() throws IOException {
                flush();
                out = null;
            }

            @Override
            public void flush() throws IOException {
                if (out == null) {
                    throw new IOException("closed");
                }

                ch.send(new CommandOutput(out.toCharArray()));
                out.reset();
            }

            @Override
            public void write(char[] b, int off, int len) throws IOException {
                if (out == null) {
                    throw new IOException("closed");
                }

                out.write(b, off, len);
            }
        });
    }
}
