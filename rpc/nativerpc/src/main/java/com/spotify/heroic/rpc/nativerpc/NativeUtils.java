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

package com.spotify.heroic.rpc.nativerpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.spotify.heroic.rpc.nativerpc.message.NativeOptions;

public abstract class NativeUtils {
    static byte[] decodeBody(final NativeOptions options, final int bodySize, final byte[] body)
            throws IOException {
        switch (options.getEncoding()) {
        case NONE:
            return body;
        case GZIP:
            return gzipDecompress(bodySize, body);
        default:
            throw new IllegalStateException("Unsupported encoding: " + options);
        }
    }

    static byte[] gzipDecompress(final int bodySize, final byte[] body) throws IOException {
        final byte[] bytes = new byte[bodySize];

        try (final GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(body))) {
            int offset = 0;

            while (offset < bodySize) {
                int read = in.read(bytes, offset, bodySize - offset);

                if (read < 0) {
                    throw new EOFException();
                }

                offset += read;
            }
        }

        return bytes;
    }

    static byte[] encodeBody(final NativeOptions options, final byte[] body) throws IOException {
        switch (options.getEncoding()) {
        case NONE:
            return body;
        case GZIP:
            return gzipCompress(body);
        default:
            throw new IllegalStateException("Unsupported encoding: " + options.getEncoding());
        }
    }

    static byte[] gzipCompress(byte[] body) throws IOException {
        try (final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            try (final GZIPOutputStream out = new GZIPOutputStream(output)) {
                out.write(body);
            }

            return output.toByteArray();
        }
    }
}
