package com.spotify.heroic.consumer;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Key;
import com.google.inject.Module;
import com.spotify.heroic.statistics.ConsumerReporter;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface ConsumerModule {
    public Module module(Key<Consumer> key, ConsumerReporter consumerReporter);

    public String id();

    public String buildId(int i);
}
