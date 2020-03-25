/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.elasticsearch;

import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import eu.toolchain.async.AsyncFramework;
import java.net.UnknownHostException;
import java.util.List;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.jetbrains.annotations.NotNull;

public class TransportClientWrapper implements ClientWrapper {
    private static final String DEFAULT_CLUSTER_NAME = "heroic";
    private static final List<String> DEFAULT_SEEDS = ImmutableList.of("localhost");
    private static final int DEFAULT_PORT = 9300;
    private static final boolean DEFAULT_SNIFF = false;
    private static final String DEFAULT_NODE_SAMPLER_INTERVAL = "30s";
    private final TransportClient client;

    @JsonCreator
    public TransportClientWrapper(
        @JsonProperty("clusterName") String clusterName,
        @JsonProperty("seeds") List<String> seeds,
        @JsonProperty("sniff") Boolean sniff,
        @JsonProperty("nodeSamplerInterval") String nodeSamplerInterval
    ) {
        List<TransportAddress> seedAddresses =
            this.parseSeeds(ofNullable(seeds).orElse(DEFAULT_SEEDS));

        /*
        client.transport.sniff: true allows for dynamically adding of new hosts and removal of old
        ones. This works by first connecting to the seed nodes and using the internal cluster
        state API to discover available nodes.
        */
        final Settings settings = Settings.builder()
            .put("cluster.name", ofNullable(clusterName).orElse(DEFAULT_CLUSTER_NAME))
            .put("client.transport.sniff", ofNullable(sniff).orElse(DEFAULT_SNIFF))
            .put("client.transport.nodes_sampler_interval",
                ofNullable(nodeSamplerInterval).orElse(DEFAULT_NODE_SAMPLER_INTERVAL))
            .build();

        this.client = new PreBuiltTransportClient(settings);
        for (final TransportAddress seed : seedAddresses) {
            client.addTransportAddress(seed);
        }
    }

    @NotNull
    @Override
    public Connection start(
        @NotNull AsyncFramework async,
        @NotNull IndexMapping index,
        @NotNull String templateName,
        @NotNull BackendType type
    ) {
        return new TransportConnection(async, index, templateName, type, client);
    }

    private List<TransportAddress> parseSeeds(final List<String> rawSeeds) {
        return ImmutableList.copyOf(rawSeeds
            .stream()
            .map(TransportClientWrapper::parseInetSocketTransportAddress)
            .iterator());
    }

    private static TransportAddress parseInetSocketTransportAddress(final String seed) {
        if (seed.contains(":")) {
            final String[] parts = seed.split(":");
            try {
                return new TransportAddress(java.net.InetAddress.getByName(parts[0]),
                    Integer.parseInt(parts[1]));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            return new TransportAddress(java.net.InetAddress.getByName(seed), DEFAULT_PORT);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String clusterName;
        private List<String> seeds;
        private Boolean sniff;
        private String nodesSamplerInterval;

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder seeds(List<String> seeds) {
            this.seeds = seeds;
            return this;
        }

        public Builder sniff(Boolean sniff) {
            this.sniff = sniff;
            return this;
        }

        public Builder nodesSamplerInterval(String nodesSamplerInterval) {
            this.nodesSamplerInterval = nodesSamplerInterval;
            return this;
        }

        public TransportClientWrapper build() {
            return new TransportClientWrapper(clusterName, seeds, sniff, nodesSamplerInterval);
        }
    }
}
