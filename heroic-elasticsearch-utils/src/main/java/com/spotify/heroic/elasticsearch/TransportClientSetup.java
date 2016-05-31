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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.UnknownHostException;
import java.util.List;

public class TransportClientSetup implements ClientSetup {
    public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
    public static final List<String> DEFAULT_SEEDS = ImmutableList.of("localhost");
    public static final int DEFAULT_PORT = 9300;

    private final String clusterName;
    private final List<InetSocketTransportAddress> seeds;

    @JsonCreator
    public TransportClientSetup(
        @JsonProperty("clusterName") String clusterName, @JsonProperty("seeds") List<String> seeds
    ) {
        this.clusterName = Optional.fromNullable(clusterName).or(DEFAULT_CLUSTER_NAME);
        this.seeds = seeds(Optional.fromNullable(seeds).or(DEFAULT_SEEDS));
    }

    @Override
    public ClientWrapper setup() throws Exception {
        final Settings settings =
            Settings.builder().put("cluster.name", clusterName).build();

        final TransportClient client = TransportClient.builder().settings(settings).build();

        for (final InetSocketTransportAddress seed : seeds) {
            client.addTransportAddress(seed);
        }
        return new ClientWrapper(client, () -> { });
    }

    private static List<InetSocketTransportAddress> seeds(final List<String> rawSeeds) {
        return ImmutableList.copyOf(rawSeeds
            .stream()
            .map(TransportClientSetup::parseInetSocketTransportAddress)
            .iterator());
    }

    private static InetSocketTransportAddress parseInetSocketTransportAddress(final String seed) {
        // TODO: handle exceptions in a cleaner way
        if (seed.contains(":")) {
            final String[] parts = seed.split(":");
            try {
                return new InetSocketTransportAddress(
                    java.net.InetAddress.getByName(parts[0]),
                    Integer.parseInt(parts[1]));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            return new InetSocketTransportAddress(
                java.net.InetAddress.getByName(seed),
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

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder seeds(List<String> seeds) {
            this.seeds = seeds;
            return this;
        }

        public TransportClientSetup build() {
            return new TransportClientSetup(clusterName, seeds);
        }
    }
}
