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
import java.net.UnknownHostException;
import java.util.List;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class TransportClientSetup implements ClientSetup {
    public static final String DEFAULT_CLUSTER_NAME = "heroic";
    public static final List<String> DEFAULT_SEEDS = ImmutableList.of("localhost");
    public static final int DEFAULT_PORT = 9300;
    public static final boolean DEFAULT_SNIFF = false;
    public static final String DEFAULT_NODE_SAMPLER_INTERVAL = "30s";

    private final String clusterName;
    private final List<TransportAddress> seeds;
    private final Boolean sniff;
    private final String nodeSamplerInterval;

    @JsonCreator
    public TransportClientSetup(
        @JsonProperty("clusterName") String clusterName,
        @JsonProperty("seeds") List<String> seeds,
        @JsonProperty("sniff") Boolean sniff,
        @JsonProperty("nodeSamplerInterval") String nodeSamplerInterval) {
        this.clusterName = ofNullable(clusterName).orElse(DEFAULT_CLUSTER_NAME);
        this.seeds = seeds(ofNullable(seeds).orElse(DEFAULT_SEEDS));
        this.sniff = ofNullable(sniff).orElse(DEFAULT_SNIFF);
        this.nodeSamplerInterval =
          ofNullable(nodeSamplerInterval).orElse(DEFAULT_NODE_SAMPLER_INTERVAL);
    }

    TransportClientSetup() {
        this.clusterName = DEFAULT_CLUSTER_NAME;
        this.seeds = seeds(DEFAULT_SEEDS);
        this.sniff = DEFAULT_SNIFF;
        this.nodeSamplerInterval = DEFAULT_NODE_SAMPLER_INTERVAL;
    }

    /*
       client.transport.sniff: true allows for dynamically adding of new hosts and removal of old
        ones. This works by first connecting to the seed nodes and using the internal cluster
        state API to discover available nodes.
     */
    @Override
    public ClientWrapper setup() throws Exception {
        final Settings settings = Settings.builder()
          .put("cluster.name", clusterName)
          .put("client.transport.sniff", sniff)
          .put("client.transport.nodes_sampler_interval", nodeSamplerInterval)
          .build();

        final TransportClient client = new PreBuiltTransportClient(settings);

        for (final TransportAddress seed : seeds) {
            client.addTransportAddress(seed);
        }
        return new ClientWrapper(client, client::close);
    }

    private static List<TransportAddress> seeds(final List<String> rawSeeds) {
        return ImmutableList.copyOf(rawSeeds
            .stream()
            .map(TransportClientSetup::parseInetSocketTransportAddress)
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
            return new TransportAddress(java.net.InetAddress.getByName(seed),
                DEFAULT_PORT);
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

        public TransportClientSetup build() {
            return new TransportClientSetup(clusterName, seeds, sniff, nodesSamplerInterval);
        }
    }
}
