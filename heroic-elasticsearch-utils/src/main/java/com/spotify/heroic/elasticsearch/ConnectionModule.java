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
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import java.util.List;

@Module
public class ConnectionModule {
    private final String clusterName;
    private final List<String> seeds;
    private final Boolean sniff;
    private final String nodeSamplerInterval;
    private final Boolean nodeClient;
    private final IndexMapping index;
    private final String templateName;
    private final ClientSetup clientSetup;

    @JsonCreator
    public ConnectionModule(
        @JsonProperty("clusterName") String clusterName,
        @JsonProperty("seeds") List<String> seeds,
        @JsonProperty("sniff") Boolean sniff,
        @JsonProperty("nodeSamplerInterval") String nodeSamplerInterval,
        @JsonProperty("nodeClient") Boolean nodeClient,
        @JsonProperty("index") IndexMapping index,
        @JsonProperty("templateName") String templateName,
        @JsonProperty("client") ClientSetup clientSetup
    ) {
        this.clusterName = ofNullable(clusterName)
            .orElse(TransportClientSetup.DEFAULT_CLUSTER_NAME);
        this.seeds = ofNullable(seeds).orElse(TransportClientSetup.DEFAULT_SEEDS);
        this.sniff = ofNullable(sniff).orElse(TransportClientSetup.DEFAULT_SNIFF);
        this.nodeSamplerInterval = ofNullable(nodeSamplerInterval).orElse(
            TransportClientSetup.DEFAULT_NODE_SAMPLER_INTERVAL);
        this.nodeClient = ofNullable(nodeClient).orElse(false);
        this.index = ofNullable(index).orElseGet(RotatingIndexMapping.builder()::build);
        // templateName defaults to the value from the backend config, and doesn't have to also
        // be set under these connection params
        this.templateName = templateName;
        this.clientSetup = ofNullable(clientSetup).orElseGet(this::defaultClientSetup);
    }

    /**
     * Setup the default client setup to be backwards compatible.
     */
    private ClientSetup defaultClientSetup() {
        if (nodeClient) {
            return new NodeClientSetup(clusterName, seeds);
        }

        return new TransportClientSetup(clusterName, seeds, sniff, nodeSamplerInterval);
    }

    public static ConnectionModule buildDefault() {
        return new ConnectionModule(null, null, null, null, null, null, null, null);
    }

    @Provides
    Provider connection(final AsyncFramework async) {
        return new Provider(async);
    }

    public class Provider {
        private final AsyncFramework async;

        @java.beans.ConstructorProperties({ "async" })
        public Provider(final AsyncFramework async) {
            this.async = async;
        }

        public Managed<Connection> construct(
            final String defaultTemplateName, final BackendType type
        ) {
            final String template = ofNullable(templateName).orElse(defaultTemplateName);

            return async.managed(new ManagedSetup<Connection>() {
                @Override
                public AsyncFuture<Connection> construct() {
                    return async.call(
                        () -> new Connection(async, index, clientSetup.setup(), template, type));
                }

                @Override
                public AsyncFuture<Void> destruct(Connection value) {
                    return value.close();
                }
            });
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String clusterName;
        private List<String> seeds;
        private Boolean sniff;
        private String nodeSamplerInterval;
        private Boolean nodeClient;
        private IndexMapping index;
        private String templateName;
        private ClientSetup clientSetup;

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

        public Builder nodeSamplerInterval(String nodeSamplerInterval) {
            this.nodeSamplerInterval = nodeSamplerInterval;
            return this;
        }

        public Builder nodeClient(Boolean nodeClient) {
            this.nodeClient = nodeClient;
            return this;
        }

        public Builder index(IndexMapping index) {
            this.index = index;
            return this;
        }

        public Builder templateName(String templateName) {
            this.templateName = templateName;
            return this;
        }

        public Builder clientSetup(ClientSetup clientSetup) {
            this.clientSetup = clientSetup;
            return this;
        }

        public ConnectionModule build() {
            return new ConnectionModule(clusterName, seeds,
              sniff, nodeSamplerInterval, nodeClient, index, templateName, clientSetup);
        }
    }
}
