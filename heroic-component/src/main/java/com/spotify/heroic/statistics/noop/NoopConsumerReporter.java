package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.ThreadPoolReporter;

public class NoopConsumerReporter implements ConsumerReporter {
    private NoopConsumerReporter() {
    }

    @Override
    public void reportMessageSize(int size) {
    }

    @Override
    public void reportMessageError() {
    }

    @Override
    public void reportConsumerSchemaError() {
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return NoopThreadPoolReporter.get();
    }

    private static final NoopConsumerReporter instance = new NoopConsumerReporter();

    public static NoopConsumerReporter get() {
        return instance;
    }
}
