package com.spotify.heroic.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class TransportClientSetup implements ClientSetup {
    public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
    public static final List<String> DEFAULT_SEEDS = ImmutableList.of("localhost");
    public static final int DEFAULT_PORT = 9300;

    private final String clusterName;
    private final List<InetSocketTransportAddress> seeds;

    @JsonCreator
    public TransportClientSetup(@JsonProperty("clusterName") String clusterName,
            @JsonProperty("seeds") List<String> seeds) {
        this.clusterName = Optional.fromNullable(clusterName).or(DEFAULT_CLUSTER_NAME);
        this.seeds = seeds(Optional.fromNullable(seeds).or(DEFAULT_SEEDS));
    }

    @Override
    public Client setup() throws Exception {
        final Settings settings = ImmutableSettings.builder().put("cluster.name", clusterName).build();

        final TransportClient client = new TransportClient(settings);

        for (final InetSocketTransportAddress seed : seeds) {
            client.addTransportAddress(seed);
        }

        return client;
    }

    @Override
    public void stop() throws Exception {
    }

    private static List<InetSocketTransportAddress> seeds(final List<String> rawSeeds) {
        final List<InetSocketTransportAddress> seeds = new ArrayList<>();

        for (final String seed : rawSeeds)
            seeds.add(parseInetSocketTransportAddress(seed));

        return seeds;
    }

    private static InetSocketTransportAddress parseInetSocketTransportAddress(final String seed) {
        if (seed.contains(":")) {
            final String parts[] = seed.split(":");
            return new InetSocketTransportAddress(parts[0], Integer.valueOf(parts[1]));
        }

        return new InetSocketTransportAddress(seed, DEFAULT_PORT);
    }
}