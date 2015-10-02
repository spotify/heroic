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

package com.spotify.heroic.metric.datastax.schema.legacy;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.spotify.heroic.metric.datastax.TypeSerializer;

public class StringSerializer implements TypeSerializer<String> {
    private static final int IS_NULL = 0x0;
    private static final int IS_EMPTY = 0x1;
    private static final int IS_STRING = 0x2;

    private static final String EMPTY = "";

    private final Charset UTF_8 = Charset.forName("UTF-8");

    @Override
    public ByteBuffer serialize(String value) {
        if (value == null) {
            final ByteBuffer b = ByteBuffer.allocate(1).put((byte) IS_NULL);

            b.flip();
            return b;
        }

        if (value.isEmpty()) {
            final ByteBuffer b = ByteBuffer.allocate(1).put((byte) IS_EMPTY);

            b.flip();
            return b;
        }

        final byte[] bytes = value.getBytes(UTF_8);
        final ByteBuffer buffer = ByteBuffer.allocate(1 + bytes.length);
        buffer.put((byte) IS_STRING);
        buffer.put(bytes);

        buffer.flip();
        return buffer;
    }

    @Override
    public String deserialize(ByteBuffer buffer) {
        final byte flag = buffer.get();

        if (flag == IS_NULL)
            return null;

        if (flag == IS_EMPTY)
            return EMPTY;

        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, UTF_8);
    }
}
