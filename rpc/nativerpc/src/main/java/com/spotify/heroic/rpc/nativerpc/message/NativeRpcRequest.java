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

package com.spotify.heroic.rpc.nativerpc.message;

import lombok.Data;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import java.io.IOException;

@Data
public class NativeRpcRequest {
    private final String endpoint;
    private final long heartbeatInterval;
    private final NativeOptions options;
    private final int size;
    private final byte[] body;

    public static NativeRpcRequest unpack(final Unpacker unpacker) throws IOException {
        final String endpoint = unpacker.readString();
        final long heartbeatInterval = unpacker.readLong();

        final NativeOptions options = NativeOptions.unpack(unpacker);

        final int size = unpacker.readInt();
        final byte[] body = unpacker.readByteArray();
        return new NativeRpcRequest(endpoint, heartbeatInterval, options, size, body);
    }

    public static void pack(final NativeRpcRequest in, final Packer out) throws IOException {
        out.write(in.getEndpoint());
        out.write(in.getHeartbeatInterval());

        // OPTIONS
        NativeOptions.pack(in.getOptions(), out);

        out.write(in.getSize());
        out.write(in.getBody());
    }
}
