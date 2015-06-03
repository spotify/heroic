package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;

public class LongSerializer implements CustomSerializer<Long> {
    @Override
    public ByteBuffer serialize(Long value) {
        final ByteBuffer buffer = ByteBuffer.allocate(8).putLong(value);
        buffer.flip();
        return buffer;
    }

    @Override
    public Long deserialize(ByteBuffer buffer) {
        return buffer.getLong();
    }
}
