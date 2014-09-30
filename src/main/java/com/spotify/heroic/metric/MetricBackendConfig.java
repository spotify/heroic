package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Key;
import com.google.inject.Module;
import com.spotify.heroic.metric.heroic.HeroicBackendConfig;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = HeroicBackendConfig.class, name = "heroic") })
public interface MetricBackendConfig {
    public Module module(Key<MetricBackend> key, String id);

    public String id();

    public String buildId(int i);
}
