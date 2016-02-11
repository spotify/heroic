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

import com.spotify.heroic.shell.protocol.Acknowledge;
import com.spotify.heroic.shell.protocol.FileClose;
import com.spotify.heroic.shell.protocol.FileFlush;
import com.spotify.heroic.shell.protocol.FileNewInputStream;
import com.spotify.heroic.shell.protocol.FileNewOutputStream;
import com.spotify.heroic.shell.protocol.FileOpened;
import com.spotify.heroic.shell.protocol.FileRead;
import com.spotify.heroic.shell.protocol.FileReadResult;
import com.spotify.heroic.shell.protocol.FileWrite;
import eu.toolchain.serializer.SerializerFramework;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

final class ServerConnection extends ShellConnection {
    public static final int BUFFER_SIZE = (1 << 16);

    public ServerConnection(final SerializerFramework framework, final Socket socket)
        throws IOException {
        super(framework, socket);
    }

    public InputStream newInputStream(Path path, StandardOpenOption... options) throws IOException {
        final FileOpened result =
            request(new FileNewInputStream(path.toString(), Arrays.asList(options)),
                FileOpened.class);

        final int handle = result.getHandle();

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
                final FileReadResult data =
                    request(new FileRead(handle, len), FileReadResult.class);
                final byte[] bytes = data.getData();
                System.arraycopy(bytes, 0, b, off, bytes.length);
                return bytes.length;
            }

            @Override
            public void close() throws IOException {
                request(new FileClose(handle), Acknowledge.class);
            }
        }, BUFFER_SIZE);
    }

    public OutputStream newOutputStream(Path path, StandardOpenOption... options)
        throws IOException {
        final FileOpened result =
            request(new FileNewOutputStream(path.toString(), Arrays.asList(options)),
                FileOpened.class);

        final int handle = result.getHandle();

        return new BufferedOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("write(int): not implemented");
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                final byte[] buffer = new byte[len];
                System.arraycopy(b, off, buffer, 0, len);
                request(new FileWrite(handle, buffer), Acknowledge.class);
            }

            @Override
            public void flush() throws IOException {
                request(new FileFlush(handle), Acknowledge.class);
            }

            @Override
            public void close() throws IOException {
                request(new FileClose(handle), Acknowledge.class);
            }
        }, BUFFER_SIZE);
    }
}
