package com.spotify.heroic.cluster.discovery;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.RequiredArgsConstructor;

import org.glassfish.jersey.client.ClientConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cluster.ClusterDiscovery;
import com.spotify.heroic.cluster.DiscoveredClusterNode;

@RequiredArgsConstructor
public class StaticListDiscovery implements ClusterDiscovery {
    private static final int DEFAULT_THREAD_POOL_SIZE = 100;

    @JsonCreator
    public static StaticListDiscovery create(
            @JsonProperty("threadPoolSize") Integer threadPoolSize,
            @JsonProperty("nodes") List<URI> nodes) {
        if (threadPoolSize == null)
            threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);

        final Executor executor = Executors.newFixedThreadPool(threadPoolSize);

        return new StaticListDiscovery(nodes, clientConfig, executor);
    }

    private final List<URI> nodes;
    private final ClientConfig config;
    private final Executor executor;

    @Override
    public Callback<Collection<DiscoveredClusterNode>> getNodes() {
        final List<DiscoveredClusterNode> discovered = new ArrayList<>();

        for (final URI n : nodes) {
            discovered.add(new DiscoveredClusterNode(n, config, executor));
        }

        return new ResolvedCallback<Collection<DiscoveredClusterNode>>(
                discovered);
    }
}
