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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.ofNullable;

@Module
@Slf4j
public class ConnectionModule {
    public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
    public static final String DEFAULT_TEMPLATE_NAME = "heroic";
    public static final int DEFAULT_CONCURRENT_BULK_REQUESTS = 5;
    public static final int DEFAULT_FLUSH_INTERVAL = 1000;
    public static final int DEFAULT_BULK_ACTIONS = 1000;
    public static final List<String> DEFAULT_SEEDS = ImmutableList.of("localhost");
    public static final Map<String, Object> DEFAULT_SETTINGS = ImmutableMap.of();

    private final String clusterName;
    private final List<String> seeds;
    private final boolean nodeClient;
    private final int concurrentBulkRequests;
    private final long flushInterval;
    private final int bulkActions;
    private final IndexMapping index;
    private final String templateName;
    private final ClientSetup clientSetup;

    @JsonCreator
    public ConnectionModule(
        @JsonProperty("clusterName") String clusterName, @JsonProperty("seeds") List<String> seeds,
        @JsonProperty("nodeClient") Boolean nodeClient,
        @JsonProperty("concurrentBulkRequests") Integer concurrentBulkRequests,
        @JsonProperty("flushInterval") Integer flushInterval,
        @JsonProperty("bulkActions") Integer bulkActions, @JsonProperty("index") IndexMapping index,
        @JsonProperty("templateName") String templateName,
        @JsonProperty("client") ClientSetup clientSetup
    ) {
        this.clusterName = ofNullable(clusterName).orElse(DEFAULT_CLUSTER_NAME);
        this.seeds = ofNullable(seeds).orElse(DEFAULT_SEEDS);
        this.nodeClient = ofNullable(nodeClient).orElse(false);
        this.concurrentBulkRequests =
            ofNullable(concurrentBulkRequests).orElse(DEFAULT_CONCURRENT_BULK_REQUESTS);
        this.flushInterval = ofNullable(flushInterval).orElse(DEFAULT_FLUSH_INTERVAL);
        this.bulkActions = ofNullable(bulkActions).orElse(DEFAULT_BULK_ACTIONS);
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
        return new ConnectionModule(null, null, null, null, null, null, null, null, null);
    }

    @Provides
    Provider connection(final AsyncFramework async, final LocalMetadataBackendReporter reporter) {
        return new Provider(async, reporter);
    }

    @RequiredArgsConstructor
    public class Provider {
        private final AsyncFramework async;
        private final LocalMetadataBackendReporter reporter;

        public Managed<Connection> construct(
            final String defaultTemplateName,
            final Map<String, Map<String, Object>> suggestedMappings
        ) {
            return construct(defaultTemplateName, suggestedMappings, DEFAULT_SETTINGS);
        }

        public Managed<Connection> construct(
            final String defaultTemplateName,
            final Map<String, Map<String, Object>> suggestedMappings,
            final Map<String, Object> settings
        ) {
            final String template = ofNullable(templateName).orElse(defaultTemplateName);
            final Map<String, Map<String, Object>> mappings =
                checkNotNull(suggestedMappings, "mappings must be configured");

            return async.managed(new ManagedSetup<Connection>() {
                @Override
                public AsyncFuture<Connection> construct() {
                    return async.call(new Callable<Connection>() {
                        @Override
                        public Connection call() throws Exception {
                            final Client client = clientSetup.setup();

                            final BulkProcessor bulk = configureBulkProcessor(client);

                            return new Connection(async, index, client, bulk, template, mappings,
                                settings);
                        }
                    });
                }

                @Override
                public AsyncFuture<Void> destruct(Connection value) {
                    final List<AsyncFuture<Void>> futures = new ArrayList<>();

                    futures.add(async.call(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            clientSetup.stop();
                            return null;
                        }
                    }));

                    futures.add(value.close());

                    return async.collectAndDiscard(futures);
                }

                private BulkProcessor configureBulkProcessor(final Client client) {
                    final BulkProcessor.Builder builder =
                        BulkProcessor.builder(client, new Listener() {
                            @Override
                            public void beforeBulk(long executionId, BulkRequest request) {
                            }

                            @Override
                            public void afterBulk(
                                long executionId, BulkRequest request, Throwable failure
                            ) {
                                reporter.reportWriteFailure(request.numberOfActions());
                                log.error("Failed to write bulk", failure);
                            }

                            @Override
                            public void afterBulk(
                                long executionId, BulkRequest request, BulkResponse response
                            ) {
                                reporter.reportWriteBatchDuration(response.getTookInMillis());

                                final int all = response.getItems().length;

                                if (!response.hasFailures()) {
                                    reporter.reportWriteSuccess(all);
                                    return;
                                }

                                final BulkItemResponse[] responses = response.getItems();
                                int failures = 0;

                                for (final BulkItemResponse r : responses) {
                                    if (r.isFailed()) {
                                        failures++;
                                    }
                                }

                                reporter.reportWriteFailure(failures);
                                reporter.reportWriteSuccess(all - failures);
                            }
                        });

                    builder.setConcurrentRequests(concurrentBulkRequests);
                    builder.setFlushInterval(new TimeValue(flushInterval));
                    builder.setBulkSize(new ByteSizeValue(-1)); // Disable bulk size
                    builder.setBulkActions(bulkActions);

                    final BulkProcessor bulkProcessor = builder.build();

                    return bulkProcessor;
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
            return new ConnectionModule(clusterName, seeds, nodeClient, concurrentBulkRequests,
                flushInterval, bulkActions, index, templateName, clientSetup);
        }
    }
};
