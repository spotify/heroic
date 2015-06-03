package com.spotify.heroic.rpc.nativerpc;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcError;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcHeartBeat;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcResponse;

import eu.toolchain.async.ResolvableFuture;

@Slf4j
@RequiredArgsConstructor
public class NativeRpcClientSessionInitializer<R> extends ChannelInitializer<Channel> {
    private final ObjectMapper mapper;
    private final Timer timer;
    private final long heartbeatInterval;
    private final int maxFrameSize;
    private final InetSocketAddress address;
    private final AtomicReference<Timeout> heartbeatTimeout;

    private final ResolvableFuture<R> future;
    private final Class<R> expected;

    @Override
    protected void initChannel(final Channel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();

        // first four bytes are length prefix of message, strip first four bytes.
        pipeline.addLast(new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
        pipeline.addLast(new NativeRpcDecoder());
        pipeline.addLast(new SimpleChannelInboundHandler<Object>() {
            @Override
            protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) throws Exception {
                if (msg instanceof NativeRpcError) {
                    final NativeRpcError error = (NativeRpcError) msg;
                    future.fail(new NativeRpcRemoteException(address, error.getMessage()));
                    ctx.channel().close();
                    return;
                }

                if (msg instanceof NativeRpcResponse) {
                    log.info("received response, cancelling heartbeat");
                    final Timeout old = heartbeatTimeout.getAndSet(null);

                    if (old != null)
                        old.cancel();

                    final NativeRpcResponse response = (NativeRpcResponse) msg;
                    final R responseBody = mapper.readValue(response.getBody(), expected);
                    future.resolve(responseBody);
                    return;
                }

                if (msg instanceof NativeRpcHeartBeat) {
                    final Timeout timeout = timer.newTimeout(heartbeatTimeout(ctx.channel(), future),
                            heartbeatInterval, TimeUnit.MILLISECONDS);
                    final Timeout old = heartbeatTimeout.getAndSet(timeout);
                    old.cancel();
                    return;
                }

                throw new IllegalArgumentException("unable to handle type: " + msg);
            }
        });

        pipeline.addLast(new SimpleChannelInboundHandler<Object>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                future.fail(cause);
            }
        });

        pipeline.addLast(new LengthFieldPrepender(4));
        pipeline.addLast(new NativeRpcEncoder());
    }

    private TimerTask heartbeatTimeout(final Channel ch, final ResolvableFuture<?> future) {
        return new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                future.fail(new Exception("request timed out (missing heartbeat)"));
                ch.close();
            }
        };
    }
}