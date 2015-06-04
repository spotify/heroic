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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcError;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcHeartBeat;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcRequest;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcResponse;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.async.Transform;

@Slf4j
@RequiredArgsConstructor
public class NativeRpcServerSessionInitializer extends ChannelInitializer<SocketChannel> {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final Timer timer;
    private final ObjectMapper mapper;
    private final NativeRpcContainer container;
    private final long heartbeatSendInterval;
    private final int maxFrameSize;

    @Override
    protected void initChannel(final SocketChannel ch) throws Exception {
        final AtomicReference<Timeout> heartbeatTimeout = new AtomicReference<>();

        final ChannelPipeline pipeline = ch.pipeline();
        // first four bytes are length prefix of message, strip first four bytes.
        pipeline.addLast(new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
        pipeline.addLast(new NativeRpcDecoder());
        pipeline.addLast(new ChannelHandler(heartbeatTimeout));

        pipeline.addLast(new LengthFieldPrepender(4));
        pipeline.addLast(new NativeRpcEncoder());
    }

    /**
     * Stop any current timeout, if pending.
     */
    private void stopCurrentTimeout(final AtomicReference<Timeout> heartbeatTimeout) {
        final Timeout old = heartbeatTimeout.getAndSet(null);

        if (old != null)
            old.cancel();
    }

    @RequiredArgsConstructor
    private class ChannelHandler extends SimpleChannelInboundHandler<Object> {
        private final AtomicReference<Timeout> heartbeatTimeout;

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) throws Exception {
            if (msg instanceof NativeRpcRequest) {
                try {
                    handleRequest(ctx.channel(), (NativeRpcRequest) msg);
                } catch (Exception e) {
                    log.error("Failed to handle request", e);
                    sendError(ctx.channel(), e.getMessage()).addListener(closeListener());
                    return;
                }

                return;
            }

            throw new IllegalArgumentException("Invalid request: " + msg);
        }

        private void handleRequest(final Channel ch, NativeRpcRequest msg) throws Exception {
            final NativeRpcRequest request = (NativeRpcRequest) msg;
            final NativeRpcContainer.EndpointSpec<Object, Object> handle = container.get(request.getEndpoint());

            if (handle == null) {
                sendError(ch, "No such endpoint: " + request.getEndpoint()).addListener(closeListener());
                return;
            }

            log.info("Request {}: {} (type: {})", request.getEndpoint(), new String(request.getBody(), UTF8),
                    handle.requestType());

            // start sending heartbeat since we are now processing a request.
            setupHeartbeat(ch);

            final Object body = mapper.readValue(request.getBody(), handle.requestType());

            final AsyncFuture<Object> handleFuture = handle.handle(body);

            // serialize in a separate thread on the async thread pool.
            // this also neatly catches errors for us in the next step.
            handleFuture.transform(serialize()).on(new FutureFinished() {
                @Override
                public void finished() throws Exception {
                    log.info("Response {}", request.getEndpoint());
                    // stop sending heartbeats when the future has been resolved.
                    // this will cause the other end to time out if a response is available, but its unable to pass the
                    // network.
                    stopCurrentTimeout(heartbeatTimeout);
                }
            }).on(sendResponseHandle(ch));
        }

        private void setupHeartbeat(final Channel ch) {
            scheduleHeartbeat(ch);

            ch.closeFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(final ChannelFuture future) throws Exception {
                    stopCurrentTimeout(heartbeatTimeout);
                }
            });
        }

        private void scheduleHeartbeat(final Channel ch) {
            final Timeout timeout = timer.newTimeout(new TimerTask() {
                @Override
                public void run(final Timeout timeout) throws Exception {
                    sendHeartbeat(ch).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(final ChannelFuture future) throws Exception {
                            scheduleHeartbeat(ch);
                        }
                    });
                }
            }, heartbeatSendInterval, TimeUnit.MILLISECONDS);

            final Timeout old = heartbeatTimeout.getAndSet(timeout);

            if (old != null)
                old.cancel();
        }

        private Transform<Object, NativeRpcResponse> serialize() {
            return new Transform<Object, NativeRpcResponse>() {
                @Override
                public NativeRpcResponse transform(Object result) throws Exception {
                    return new NativeRpcResponse(mapper.writeValueAsBytes(result));
                }
            };
        }

        private FutureDone<NativeRpcResponse> sendResponseHandle(final Channel ch) {
            return new FutureDone<NativeRpcResponse>() {
                @Override
                public void cancelled() throws Exception {
                    ch.writeAndFlush(new NativeRpcError("request cancelled")).addListener(closeListener());
                }

                @Override
                public void failed(final Throwable e) throws Exception {
                    ch.writeAndFlush(new NativeRpcError(e.getMessage())).addListener(closeListener());
                }

                @Override
                public void resolved(final NativeRpcResponse response) throws Exception {
                    sendHeartbeat(ch).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture f) throws Exception {
                            if (!f.isSuccess()) {
                                sendError(ch,
                                        f.cause() == null ? "send of tail heartbeat failed" : f.cause().getMessage())
                                        .addListener(closeListener());
                                return;
                            }

                            ch.writeAndFlush(response).addListener(closeListener());
                        }
                    });
                }
            };
        }

        private ChannelFuture sendHeartbeat(final Channel ch) {
            return ch.writeAndFlush(new NativeRpcHeartBeat());
        }

        private ChannelFuture sendError(final Channel ch, final String error) {
            return ch.writeAndFlush(new NativeRpcError(error));
        }

        /**
         * Listener that shuts down the connection after its promise has been resolved.
         */
        private ChannelFutureListener closeListener() {
            // immediately stop sending heartbeats.
            stopCurrentTimeout(heartbeatTimeout);

            return new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    future.channel().close();
                }
            };
        }
    }
};
