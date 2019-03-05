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
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import java.util.List;
import java.util.Map;

@Module
public class ConnectionModule {
    public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
    public static final List<String> DEFAULT_SEEDS = ImmutableList.of("localhost");
    public static final Map<String, Object> DEFAULT_SETTINGS = ImmutableMap.of();

    private final String clusterName;
    private final List<String> seeds;
    private final boolean nodeClient;
    private final IndexMapping index;
    private final String templateName;
    private final ClientSetup clientSetup;

    @JsonCreator
    public ConnectionModule(
        @JsonProperty("clusterName") String clusterName, @JsonProperty("seeds") List<String> seeds,
        @JsonProperty("nodeClient") Boolean nodeClient, @JsonProperty("index") IndexMapping index,
        @JsonProperty("templateName") String templateName,
        @JsonProperty("client") ClientSetup clientSetup
    ) {
        this.clusterName = ofNullable(clusterName).orElse(DEFAULT_CLUSTER_NAME);
        this.seeds = ofNullable(seeds).orElse(DEFAULT_SEEDS);
        this.nodeClient = ofNullable(nodeClient).orElse(false);
        this.index = ofNullable(index).orElseGet(RotatingIndexMapping.builder()::build);
        this.templateName = templateName;
        this.clientSetup = ofNullable(clientSetup).orElseGet(this::defaultClientSetup);
    }

    /**
     * Setup the defualt client setup to be backwards compatible.
     */
    private ClientSetup defaultClientSetup() {
        if (nodeClient) {
            return new NodeClientSetup(clusterName, seeds);
        }

        return new TransportClientSetup(clusterName, seeds);
    }

    public static ConnectionModule buildDefault() {
        return new ConnectionModule(null, null, null, null, null, null);
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
        private Boolean nodeClient;
        private Integer concurrentBulkRequests;
        private Integer flushInterval;
        private Integer bulkActions;
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

        public Builder nodeClient(Boolean nodeClient) {
            this.nodeClient = nodeClient;
            return this;
        }

        public Builder nodeClient(Integer concurrentBulkRequests) {
            this.concurrentBulkRequests = concurrentBulkRequests;
            return this;
        }

        public Builder flushInterval(Integer flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder bulkActions(Integer bulkActions) {
            this.bulkActions = bulkActions;
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
            return new ConnectionModule(clusterName, seeds, nodeClient, index, templateName,
                clientSetup);
        }
    }
};
