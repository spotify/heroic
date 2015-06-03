package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import org.apache.commons.lang3.tuple.Pair;

@RequiredArgsConstructor
public class MapSerializer<A, B> implements CustomSerializer<Map<A, B>> {
    private final CustomSerializer<A> a;
    private final CustomSerializer<B> b;

    @Override
    public ByteBuffer serialize(final Map<A, B> value) {
        final List<Pair<ByteBuffer, ByteBuffer>> buffers = new ArrayList<>();

        short size = 0;

        for (final Map.Entry<A, B> e : value.entrySet()) {
            final ByteBuffer key = a.serialize(e.getKey());
            final ByteBuffer val = b.serialize(e.getValue());

            size += key.limit() + val.limit();

            buffers.add(Pair.of(key, val));
        }

        final ByteBuffer buffer = ByteBuffer.allocate(4 + 8 * value.size() + size);
        buffer.putShort((short) buffers.size());

        for (final Pair<ByteBuffer, ByteBuffer> p : buffers) {
            buffer.putShort((short) p.getLeft().remaining());
            buffer.put(p.getLeft());
            buffer.putShort((short) p.getRight().remaining());
            buffer.put(p.getRight());
        }

        buffer.flip();
        return buffer;
    }

    @Override
    public Map<A, B> deserialize(ByteBuffer buffer) {
        final short len = buffer.getShort();

        final Map<A, B> map = new LinkedHashMap<>();

        for (short i = 0; i < len; i++) {
            final A key = next(buffer, a);
            final B value = next(buffer, b);
            map.put(key, value);
        }

        return map;
    }

    private <T> T next(ByteBuffer buffer, CustomSerializer<T> serializer) {
        final short segment = buffer.getShort();
        final ByteBuffer slice = buffer.slice();
        slice.limit(segment);
        final T value = serializer.deserialize(slice);

        buffer.position(buffer.position() + segment);
        return value;
    }
}
