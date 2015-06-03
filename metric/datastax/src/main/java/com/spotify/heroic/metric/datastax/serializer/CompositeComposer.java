package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CompositeComposer {
    private static final byte EQ = 0x0;

    final List<ByteBuffer> buffers = new ArrayList<>();
    short size = 0;

    public <T> void add(T key, CustomSerializer<T> s) {
        final ByteBuffer buffer = s.serialize(key);
        buffers.add(buffer);
        size += buffer.limit();
    }

    public ByteBuffer serialize() {
        final ByteBuffer buffer = ByteBuffer.allocate(buffers.size() * 5 + size);

        for (final ByteBuffer b : buffers) {
            buffer.putShort((short) b.limit());
            buffer.put(b);
            buffer.put(EQ);
        }

        buffer.flip();
        return buffer;
    }
}
