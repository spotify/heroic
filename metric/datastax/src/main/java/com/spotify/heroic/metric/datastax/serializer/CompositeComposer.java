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

package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CompositeComposer {
    private static final byte EQ = 0x0;

    final List<ByteBuffer> buffers = new ArrayList<>();
    short size = 0;

    public <T> void add(T key, CustomSerializer<T> s) {
        final ByteBuffer buffer = s.serialize(key);
        buffers.add(buffer);
        size += buffer.limit();
    }

    public ByteBuffer serialize() {
        final ByteBuffer buffer = ByteBuffer.allocate(buffers.size() * 5 + size);

        for (final ByteBuffer b : buffers) {
            buffer.putShort((short) b.limit());
            buffer.put(b);
            buffer.put(EQ);
        }

        buffer.flip();
        return buffer;
    }
}
