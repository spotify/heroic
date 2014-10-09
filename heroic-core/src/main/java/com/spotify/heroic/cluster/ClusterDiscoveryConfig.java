package com.spotify.heroic.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Key;
import com.google.inject.Module;
import com.spotify.heroic.cluster.discovery.StaticListDiscoveryConfig;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = StaticListDiscoveryConfig.class, name = "static") })
public interface ClusterDiscoveryConfig {
    public Module module(final Key<ClusterDiscovery> key);
}
