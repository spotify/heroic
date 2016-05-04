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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.rpc.nativerpc.message.NativeOptions;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcError;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcHeartBeat;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcRequest;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcResponse;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.Transform;
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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
public class NativeRpcServerSession extends ChannelInitializer<SocketChannel> {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final Timer timer;
    private final ObjectMapper mapper;
    private final NativeRpcContainer container;
    private final int maxFrameSize;
    private final NativeEncoding encoding;

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

        if (old != null) {
            old.cancel();
        }
    }

    @RequiredArgsConstructor
    private class ChannelHandler extends SimpleChannelInboundHandler<Object> {
        private final AtomicReference<Timeout> heartbeatTimeout;

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final Object msg)
            throws Exception {
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

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("{}: exception in channel, closing", ctx.channel(), cause);
            stopCurrentTimeout(heartbeatTimeout);
            ctx.channel().close();
        }

        private void handleRequest(final Channel ch, NativeRpcRequest msg) throws Exception {
            final NativeRpcRequest request = (NativeRpcRequest) msg;
            final NativeRpcContainer.EndpointSpec<Object, Object> handle =
                container.get(request.getEndpoint());

            if (handle == null) {
                sendError(ch, "No such endpoint: " + request.getEndpoint()).addListener(
                    closeListener());
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace("request[{}:{}ms] {}", request.getEndpoint(),
                    request.getHeartbeatInterval(), new String(request.getBody(), UTF8));
            }

            final long heartbeatInterval = calculcateHeartbeatInterval(msg);

            if (heartbeatInterval > 0) {
                // start sending heartbeat since we are now processing a request.
                setupHeartbeat(ch, heartbeatInterval);
            }

            final byte[] bytes =
                NativeUtils.decodeBody(request.getOptions(), request.getSize(), request.getBody());

            final Object body = mapper.readValue(bytes, handle.requestType());

            final AsyncFuture<Object> handleFuture = handle.handle(body);

            // Serialize in a separate thread on the async thread pool.
            // this also neatly catches errors for us in the next step.
            // Stop sending heartbeats immediately when the future has been finished.
            // this will cause the other end to time out if a response is available, but its unable
            // to pass the network.
            handleFuture
                .directTransform(serialize(request))
                .onFinished(() -> stopCurrentTimeout(heartbeatTimeout))
                .onDone(sendResponseHandle(ch));
        }

        private long calculcateHeartbeatInterval(NativeRpcRequest msg) {
            if (msg.getHeartbeatInterval() <= 0) {
                return 0;
            }

            return msg.getHeartbeatInterval() / 2;
        }

        private void setupHeartbeat(final Channel ch, final long heartbeatInterval) {
            scheduleHeartbeat(ch, heartbeatInterval);

            ch.closeFuture().addListener(future -> {
                stopCurrentTimeout(heartbeatTimeout);
            });
        }

        private void scheduleHeartbeat(final Channel ch, final long heartbeatInterval) {
            final Timeout timeout = timer.newTimeout(t -> {
                sendHeartbeat(ch).addListener((final ChannelFuture future) -> {
                    scheduleHeartbeat(ch, heartbeatInterval);
                });
            }, heartbeatInterval, TimeUnit.MILLISECONDS);

            final Timeout old = heartbeatTimeout.getAndSet(timeout);

            if (old != null) {
                old.cancel();
            }
        }

        private Transform<Object, NativeRpcResponse> serialize(final NativeRpcRequest request) {
            return (Object result) -> {
                byte[] body = mapper.writeValueAsBytes(result);

                if (log.isTraceEnabled()) {
                    log.trace("response[{}]: {}", request.getEndpoint(), new String(body, UTF8));
                }

                final int bodySize = body.length;
                final NativeOptions options = new NativeOptions(encoding);
                return new NativeRpcResponse(options, bodySize,
                    NativeUtils.encodeBody(options, body));
            };
        }

        private FutureDone<NativeRpcResponse> sendResponseHandle(final Channel ch) {
            return new FutureDone<NativeRpcResponse>() {
                @Override
                public void cancelled() throws Exception {
                    log.error("{}: request cancelled", ch);
                    ch
                        .writeAndFlush(new NativeRpcError("request cancelled"))
                        .addListener(closeListener());
                }

                @Override
                public void failed(final Throwable e) throws Exception {
                    log.error("{}: request failed", ch, e);
                    ch
                        .writeAndFlush(new NativeRpcError(e.getMessage()))
                        .addListener(closeListener());
                }

                @Override
                public void resolved(final NativeRpcResponse response) throws Exception {
                    sendHeartbeat(ch).addListener(f -> {
                        if (!f.isSuccess()) {
                            sendError(ch, f.cause() == null ? "send of tail heartbeat failed"
                                : f.cause().getMessage()).addListener(closeListener());
                            return;
                        }

                        ch.writeAndFlush(response).addListener(closeListener());
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

            return future -> future.channel().close();
        }
    }
};
