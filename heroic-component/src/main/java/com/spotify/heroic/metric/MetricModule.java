package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Key;
import com.google.inject.Module;

@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface MetricModule {
    public Module module(Key<MetricBackend> key, String id);

    public String id();

    public String buildId(int i);
}
