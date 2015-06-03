package com.spotify.heroic.rpc.nativerpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

import com.spotify.heroic.rpc.nativerpc.message.NativeRpcError;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcHeartBeat;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcRequest;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcResponse;

public class NativeRpcEncoder extends MessageToByteEncoder<Object> {
    private final MessagePack messagePack = new MessagePack();

    @Override
    protected void encode(final ChannelHandlerContext ctx, final Object in, final ByteBuf out) throws Exception {
        try (final ByteBufOutputStream stream = new ByteBufOutputStream(out)) {
            try (final Packer packer = messagePack.createPacker(stream)) {
                if (in instanceof NativeRpcHeartBeat) {
                    encodeHeartbeat((NativeRpcHeartBeat) in, packer);
                    return;
                }

                if (in instanceof NativeRpcRequest) {
                    encodeRequest((NativeRpcRequest) in, packer);
                    return;
                }

                if (in instanceof NativeRpcResponse) {
                    encodeResponse((NativeRpcResponse) in, packer);
                    return;
                }

                if (in instanceof NativeRpcError) {
                    encodeErrorResponse((NativeRpcError) in, packer);
                    return;
                }
            }
        }

        throw new IllegalArgumentException("Unable to encode object: " + in);
    }

    private void encodeHeartbeat(final NativeRpcHeartBeat in, final Packer out) throws IOException {
        out.write(NativeRpc.HEARTBEAT);
    }

    private void encodeRequest(final NativeRpcRequest in, final Packer out) throws IOException {
        out.write(NativeRpc.REQUEST);
        out.write(in.getEndpoint());
        out.write(in.getBody());
    }

    private void encodeResponse(final NativeRpcResponse in, final Packer out) throws IOException {
        out.write(NativeRpc.RESPONSE);
        out.write(in.getBody());
    }

    private void encodeErrorResponse(final NativeRpcError in, final Packer out) throws IOException {
        out.write(NativeRpc.ERR_RESPONSE);
        out.write(in.getMessage());
    }
}