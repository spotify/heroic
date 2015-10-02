package com.spotify.heroic.metric.datastax;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface TypeSerializer<T> {
    ByteBuffer serialize(T value) throws IOException;

    T deserialize(ByteBuffer buffer) throws IOException;
}