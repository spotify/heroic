package com.spotify.heroic.cluster.discovery;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Named;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.cluster.ClusterDiscovery;
import com.spotify.heroic.cluster.ClusterDiscoveryConfig;

@RequiredArgsConstructor
public class StaticListDiscoveryConfig implements ClusterDiscoveryConfig {
    private static final int DEFAULT_THREAD_POOL_SIZE = 100;

    private final List<URI> nodes;

    @JsonCreator
    public static StaticListDiscoveryConfig create(
            @JsonProperty("threadPoolSize") Integer threadPoolSize,
            @JsonProperty("nodes") List<URI> nodes) {
        if (threadPoolSize == null)
            threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

        if (nodes == null)
            nodes = new ArrayList<>();

        return new StaticListDiscoveryConfig(nodes);
    }

    @Override
    public Module module(final Key<ClusterDiscovery> key) {
        return new PrivateModule() {
            @Provides
            @Named("nodes")
            public List<URI> nodes() {
                return nodes;
            }

            @Override
            protected void configure() {
                bind(key).to(StaticListDiscovery.class);
                expose(key);
            }
        };
    }
}
