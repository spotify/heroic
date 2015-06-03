package com.spotify.heroic.statistics;

public interface ConsumerReporter {
    void reportMessageSize(int size);

    void reportMessageError();

    void reportConsumerSchemaError();

    ThreadPoolReporter newThreadPool();
}
