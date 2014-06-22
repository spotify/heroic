package com.spotify.heroic.consumer;

public interface ConsumerSchema {
    public void consume(Consumer consumer, byte[] message)
            throws ConsumerSchemaException;
}
