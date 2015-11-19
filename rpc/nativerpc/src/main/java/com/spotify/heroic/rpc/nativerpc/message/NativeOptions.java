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

import java.io.IOException;

import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import com.spotify.heroic.rpc.nativerpc.NativeEncoding;

import lombok.Data;

/**
 * A dynamic set of options that are part of a native rpc request.
 */
@Data
public class NativeOptions {
    /**
     * Indicates the encoding that the body is encoded using.
     */
    static final String ENCODING = "encoding";

    private final NativeEncoding encoding;

    public static NativeOptions unpack(final Unpacker unpacker) throws IOException {
        NativeEncoding encoding = NativeEncoding.NONE;

        final int size = unpacker.readInt();

        for (int i = 0; i < size; i++) {
            switch (unpacker.readString()) {
            case ENCODING:
                encoding = NativeEncoding.valueOf(unpacker.readString());
                break;
            default: // ignore unknown options
                break;
            }
        }

        return new NativeOptions(encoding);
    }

    public static void pack(NativeOptions options, Packer out) throws IOException {
        // number of options
        out.write(1);

        out.write(ENCODING);
        out.write(options.getEncoding().toString());
    }
}
