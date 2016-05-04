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

import com.spotify.heroic.rpc.nativerpc.message.NativeRpcError;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcHeartBeat;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcRequest;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

public class NativeRpcEncoder extends MessageToByteEncoder<Object> {
    private final MessagePack messagePack = new MessagePack();

    @Override
    protected void encode(final ChannelHandlerContext ctx, final Object in, final ByteBuf out)
        throws Exception {
        try (final ByteBufOutputStream stream = new ByteBufOutputStream(out)) {
            try (final Packer packer = messagePack.createPacker(stream)) {
                if (in instanceof NativeRpcHeartBeat) {
                    packer.write(NativeRpc.HEARTBEAT);
                    NativeRpcHeartBeat.pack((NativeRpcHeartBeat) in, packer);
                    return;
                }

                if (in instanceof NativeRpcRequest) {
                    packer.write(NativeRpc.REQUEST);
                    NativeRpcRequest.pack((NativeRpcRequest) in, packer);
                    return;
                }

                if (in instanceof NativeRpcResponse) {
                    packer.write(NativeRpc.RESPONSE);
                    NativeRpcResponse.pack((NativeRpcResponse) in, packer);
                    return;
                }

                if (in instanceof NativeRpcError) {
                    packer.write(NativeRpc.ERR_RESPONSE);
                    NativeRpcError.pack((NativeRpcError) in, packer);
                    return;
                }
            }
        }

        throw new IllegalArgumentException("Unable to encode object: " + in);
    }
}
