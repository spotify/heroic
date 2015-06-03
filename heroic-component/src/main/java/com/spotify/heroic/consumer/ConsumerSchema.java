package com.spotify.heroic.consumer;

import com.spotify.heroic.exceptions.ConsumerSchemaException;

public interface ConsumerSchema {
    public void consume(Consumer consumer, byte[] message) throws ConsumerSchemaException;
}
