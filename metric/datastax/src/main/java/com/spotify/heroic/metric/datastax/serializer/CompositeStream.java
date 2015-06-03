package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CompositeStream {
    private static final byte EQ = 0x0;

    private final ByteBuffer buffer;

    public <T> T next(CustomSerializer<T> serializer) {
        final short segment = buffer.getShort();
        final ByteBuffer part = buffer.slice();
        part.limit(segment);

        final T result = serializer.deserialize(part);

        buffer.position(buffer.position() + segment);

        if (buffer.get() != EQ)
            throw new IllegalStateException("Illegal state in ComponentReader, expected EQ (0)");

        return result;
    }
}