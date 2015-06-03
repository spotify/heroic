package com.spotify.heroic.consumer;

import com.google.inject.Key;
import com.google.inject.Module;
import com.spotify.heroic.statistics.ConsumerReporter;

public interface ConsumerModule {
    public Module module(Key<Consumer> key, ConsumerReporter consumerReporter);

    public String id();

    public String buildId(int i);
}
