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
import com.google.common.base.Optional;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.net.InetAddress;
import java.util.List;

public class NodeClientSetup implements ClientSetup {
    public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";

    private final String clusterName;
    private final String[] seeds;

    @JsonCreator
    public NodeClientSetup(String clusterName, List<String> seeds) {
        this.clusterName = Optional.fromNullable(clusterName).or(DEFAULT_CLUSTER_NAME);
        this.seeds = seedsToDiscovery(seeds);
    }

    @Override
    public ClientWrapper setup() throws Exception {
        final Settings settings = Settings
            .builder()
            .put("node.name", InetAddress.getLocalHost().getHostName())
            .put("discovery.zen.ping.multicast.enabled", false)
            .putArray("discovery.zen.ping.unicast.hosts", seeds)
            .build();

        final Node node = NodeBuilder
            .nodeBuilder()
            .settings(settings)
            .client(true)
            .clusterName(clusterName)
            .node();

        return new ClientWrapper(node.client(), node::close);
    }

    private String[] seedsToDiscovery(List<String> seeds) {
        return seeds.toArray(new String[0]);
    }
}
