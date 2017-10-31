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

import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SharedPool;
import eu.toolchain.serializer.io.AbstractSerialReader;
import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/*
 * Identical to eu.toolchain.serializer.io.CoreByteBufferSerialReader, except that this
 * implementation will update the offset in the provided ByteBuffer as the reader progresses.
 */
public class BigtableByteBufferSerialReader extends AbstractSerialReader {
    private final ByteBuffer buffer;

    public BigtableByteBufferSerialReader(
        SharedPool pool, Serializer<Integer> scopeSize, ByteBuffer buffer
    ) {
        super(pool, scopeSize);
        this.buffer = buffer;
    }

    public byte read() throws IOException {
        try {
            return this.buffer.get();
        } catch (BufferUnderflowException var2) {
            throw new EOFException();
        }
    }

    public void read(byte[] bytes, int offset, int length) throws IOException {
        try {
            this.buffer.get(bytes, offset, length);
        } catch (BufferUnderflowException var5) {
            throw new EOFException();
        }
    }

    public void skip(int length) throws IOException {
        try {
            this.buffer.position(this.buffer.position() + length);
        } catch (IllegalArgumentException var3) {
            throw new EOFException();
        }
    }
}
