/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.heroic.metric.bigtable;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

/**
 * Custom string serializer to be backwards compatible with our previous serialization.
 */
@RequiredArgsConstructor
public class CustomStringSerializer implements Serializer<String> {
    private final Serializer<Integer> size;

    private static final Charset UTF_8 = Charset.forName("utf8");

    @Override
    public void serialize(SerialWriter buffer, String value) throws IOException {
        final CharsetEncoder encoder = UTF_8.newEncoder();
        /* allocate a worst-case buffer */
        final int worst = (int) (value.length() * encoder.maxBytesPerChar());
        final ByteBuffer bytes = buffer.pool().allocate(worst);

        try {
            encoder.encode(CharBuffer.wrap(value), bytes, true);
            bytes.flip();

            this.size.serialize(buffer, bytes.remaining());
            this.size.serialize(buffer, value.length());

            buffer.write(bytes);
        } finally {
            buffer.pool().release(worst);
        }
    }

    @Override
    public String deserialize(SerialReader buffer) throws IOException {
        final CharsetDecoder decoder = UTF_8.newDecoder();

        final int length = this.size.deserialize(buffer);
        final int size = this.size.deserialize(buffer);

        final ByteBuffer bytes = buffer.pool().allocate(length);

        try {
            buffer.read(bytes);
            bytes.flip();

            final char[] chars = new char[size];
            decoder.decode(bytes, CharBuffer.wrap(chars), true);
            return new String(chars);
        } finally {
            buffer.pool().release(length);
        }
    }
}
