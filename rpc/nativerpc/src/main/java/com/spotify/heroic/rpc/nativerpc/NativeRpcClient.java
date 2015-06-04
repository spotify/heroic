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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcRequest;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;

@RequiredArgsConstructor
@ToString(of = { "address", "sendTimeout", "heartbeatInterval" })
public class NativeRpcClient {
    private final AsyncFramework async;
    private final EventLoopGroup group;
    private final int maxFrameSize;
    private final InetSocketAddress address;
    private final ObjectMapper mapper;
    private final Timer timer;
    private final long sendTimeout;
    private final long heartbeatInterval;

    private static final RpcEmptyBody EMPTY = new RpcEmptyBody();

    public <Q, R> AsyncFuture<R> request(final String endpoint, final Q body, final Class<R> expected) {
        final byte[] requestBody;

        try {
            requestBody = mapper.writeValueAsBytes(body);
        } catch (JsonProcessingException e) {
            return async.failed(e);
        }

        final NativeRpcRequest request = new NativeRpcRequest(endpoint, requestBody);
        final ResolvableFuture<R> future = async.future();
        final AtomicReference<Timeout> heartbeatTimeout = new AtomicReference<>();

        final Bootstrap b = new Bootstrap();
        b.channel(NioSocketChannel.class);
        b.group(group);
        b.handler(new NativeRpcClientSessionInitializer<R>(mapper, timer, heartbeatInterval, maxFrameSize, address,
                heartbeatTimeout, future, expected));

        // timeout for how long we are allowed to spend attempting to send a request.
        final Timeout sendTimeout = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                future.fail(new Exception("sending of request timed out"));
            }
        }, this.sendTimeout, TimeUnit.MILLISECONDS);

        b.connect(address).addListener(handleConnect(request, future, heartbeatTimeout, sendTimeout));

        return future;
    }

    private <R> ChannelFutureListener handleConnect(final NativeRpcRequest request, final ResolvableFuture<R> future,
            final AtomicReference<Timeout> heartbeatTimeout, final Timeout requestTimeout) {
        return new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                if (!f.isSuccess()) {
                    future.fail(f.cause());
                    return;
                }

                f.channel().writeAndFlush(request)
                        .addListener(handleRequestSent(future, heartbeatTimeout, requestTimeout));
            }
        };
    }

    private <R> ChannelFutureListener handleRequestSent(final ResolvableFuture<R> future,
            final AtomicReference<Timeout> heartbeatTimeout, final Timeout requestTimeout) {
        return new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                requestTimeout.cancel();

                if (!f.isSuccess()) {
                    future.fail(f.cause());
                    return;
                }

                final Timeout timeout = timer.newTimeout(heartbeatTimeout(f.channel(), future), heartbeatInterval,
                        TimeUnit.MILLISECONDS);

                heartbeatTimeout.set(timeout);
            }
        };
    }

    private TimerTask heartbeatTimeout(final Channel ch, final ResolvableFuture<?> future) {
        return new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                future.fail(new Exception("missing heartbeat, request timed out"));
                ch.close();
            }
        };
    }

    public <R> AsyncFuture<R> request(String endpoint, Class<R> expected) {
        return request(endpoint, EMPTY, expected);
    }
}