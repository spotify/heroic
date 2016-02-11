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

import com.spotify.heroic.metric.datastax.TypeSerializer;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.ByteBuffer;

@RequiredArgsConstructor
public class CompositeStream {
    private static final byte EQ = 0x0;

    private final ByteBuffer buffer;

    public <T> T next(TypeSerializer<T> serializer) throws IOException {
        final short segment = buffer.getShort();
        final ByteBuffer part = buffer.slice();
        part.limit(segment);

        final T result = serializer.deserialize(part);

        buffer.position(buffer.position() + segment);

        if (buffer.get() != EQ) {
            throw new IllegalStateException("Illegal state in ComponentReader, expected EQ (0)");
        }

        return result;
    }
}
