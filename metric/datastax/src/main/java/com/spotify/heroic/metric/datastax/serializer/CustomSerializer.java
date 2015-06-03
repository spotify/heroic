package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;

public interface CustomSerializer<T> {
    public ByteBuffer serialize(T value);

    public T deserialize(ByteBuffer buffer);
}
