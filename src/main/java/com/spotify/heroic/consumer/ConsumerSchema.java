package com.spotify.heroic.consumer;

import com.spotify.heroic.consumer.exceptions.SchemaException;

public interface ConsumerSchema {
    public void consume(Consumer consumer, byte[] message)
            throws SchemaException;
}
