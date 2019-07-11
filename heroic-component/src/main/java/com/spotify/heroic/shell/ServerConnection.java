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

import com.google.protobuf.ByteString;
import com.spotify.heroic.proto.ShellMessage.FileEvent;
import com.spotify.heroic.proto.ShellMessage.FileEvent.Event;
import com.spotify.heroic.proto.ShellMessage.FileStream.Type;
import com.spotify.heroic.proto.ShellMessage.Message;
import com.spotify.heroic.shell.protocol.MessageBuilder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Path;
import org.slf4j.Logger;

final class ServerConnection extends ShellConnection {
    public static final int BUFFER_SIZE = (1 << 16);
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ServerConnection.class);

    public ServerConnection(final Socket socket)
        throws IOException {
        super(socket);
    }

    private int newFileStream(Path path, Type type) throws IOException {
        final Message response = request(MessageBuilder.fileStream(path.toString(), type));
        FileEvent result = null;
        if (response.hasFileEvent()) {
            result = response.getFileEvent();
        }

        if (result == null || result.getEvent() != Event.OPENED) {
            throw new IOException("Got unexpected message opening file stream");
        }

        return result.getHandle();
    }

    public InputStream newInputStream(Path path) throws IOException {
        final int handle = newFileStream(path, Type.INPUT);

        /* BufferedInputStream should _never_ use {@link InputStream#read()}, making sure we don't
         * have to implement that method. Reading one byte at-a-time over the network would be a
         * folly */
        return new BufferedInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("read(): not implemented");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                final Message response = request(MessageBuilder.fileRead(handle, len));
                if (!response.hasFileReadResult()) {
                    throw new IOException("Got unexpected message reading file");
                }

                final byte[] bytes = response.getFileReadResult().getDataBytes().toByteArray();
                System.arraycopy(bytes, 0, b, off, bytes.length);
                return bytes.length;
            }

            @Override
            public void close() throws IOException {
                ackedRequest(MessageBuilder.fileEvent(handle, Event.CLOSE));
            }
        }, BUFFER_SIZE);
    }

    public OutputStream newOutputStream(Path path) throws IOException {
        final int handle = newFileStream(path, Type.OUTPUT);

        return new BufferedOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("write(int): not implemented");
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                send(MessageBuilder.fileWrite(handle, ByteString.copyFrom(b, 0, len)));
            }

            @Override
            public void flush() throws IOException {
                ackedRequest(MessageBuilder.fileEvent(handle, Event.FLUSH));
            }

            @Override
            public void close() throws IOException {
                ackedRequest(MessageBuilder.fileEvent(handle, Event.CLOSE));
            }
        }, BUFFER_SIZE);
    }
}
