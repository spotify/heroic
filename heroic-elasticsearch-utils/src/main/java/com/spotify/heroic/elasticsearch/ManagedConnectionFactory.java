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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;

@Slf4j
public class ManagedConnectionFactory {
    public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
    public static final String DEFAULT_TEMPLATE_NAME = "heroic";
    public static final int DEFAULT_CONCURRENT_BULK_REQUESTS = 5;
    public static final int DEFAULT_FLUSH_INTERVAL = 1000;
    public static final int DEFAULT_BULK_ACTIONS = 1000;
    public static final List<String> DEFAULT_SEEDS = ImmutableList.of("localhost");
    public static final Map<String, XContentBuilder> EMPTY_MAPPINGS = ImmutableMap.of();

    @Inject
    private LocalMetadataBackendReporter reporter;

    @Inject
    private AsyncFramework async;

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
    public ManagedConnectionFactory(@JsonProperty("clusterName") String clusterName,
            @JsonProperty("seeds") List<String> seeds, @JsonProperty("nodeClient") Boolean nodeClient,
            @JsonProperty("concurrentBulkRequests") Integer concurrentBulkRequests,
            @JsonProperty("flushInterval") Integer flushInterval, @JsonProperty("bulkActions") Integer bulkActions,
            @JsonProperty("index") IndexMapping index, @JsonProperty("templateName") String templateName,
            @JsonProperty("client") ClientSetup clientSetup) {
        this.clusterName = Optional.fromNullable(clusterName).or(DEFAULT_CLUSTER_NAME);
        this.seeds = Optional.fromNullable(seeds).or(DEFAULT_SEEDS);
        this.nodeClient = Optional.fromNullable(nodeClient).or(false);
        this.concurrentBulkRequests = Optional.fromNullable(concurrentBulkRequests)
                .or(DEFAULT_CONCURRENT_BULK_REQUESTS);
        this.flushInterval = Optional.fromNullable(flushInterval).or(DEFAULT_FLUSH_INTERVAL);
        this.bulkActions = Optional.fromNullable(bulkActions).or(DEFAULT_BULK_ACTIONS);
        this.index = Optional.fromNullable(index).or(RotatingIndexMapping.defaultSupplier());
        this.templateName = templateName;
        this.clientSetup = Optional.fromNullable(clientSetup).or(defaultClientSetup());
    }

    /**
     * Setup the defualt client setup to be backwards compatible.
     */
    private Supplier<ClientSetup> defaultClientSetup() {
        return new Supplier<ClientSetup>() {
            @Override
            public ClientSetup get() {
                if (nodeClient)
                    return new NodeClientSetup(clusterName, seeds);

                return new TransportClientSetup(clusterName, seeds);
            }
        };
    }

    public static ManagedConnectionFactory buildDefault() {
        return new ManagedConnectionFactory(null, null, null, null, null, null, null, null, null);
    }

    public static Supplier<ManagedConnectionFactory> provideDefault() {
        return new Supplier<ManagedConnectionFactory>() {
            @Override
            public ManagedConnectionFactory get() {
                return buildDefault();
            }
        };
    }

    public Managed<Connection> construct(final String defaultTemplateName,
            final Map<String, XContentBuilder> suggestedMappings) {
        final String templateName = Optional.fromNullable(this.templateName).or(defaultTemplateName);
        final Map<String, XContentBuilder> mappings = checkNotNull(suggestedMappings, "mappings must be configured");

        return async.managed(new ManagedSetup<Connection>() {
            @Override
            public AsyncFuture<Connection> construct() {
                return async.call(new Callable<Connection>() {
                    @Override
                    public Connection call() throws Exception {
                        final Client client = clientSetup.setup();

                        configureMapping(client, templateName, mappings);

                        final BulkProcessor bulk = configureBulkProcessor(client);

                        return new Connection(async, index, client, bulk);
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
        });
    }

    private void configureMapping(Client client, String templateName, Map<String, XContentBuilder> mappings)
            throws Exception {
        final IndicesAdminClient indices = client.admin().indices();

        if (isTemplateUpToDate(indices, templateName, mappings))
            return;

        createTemplate(indices, templateName, mappings);
    }

    private boolean isTemplateUpToDate(IndicesAdminClient indices, String templateName,
            Map<String, XContentBuilder> mappings) throws Exception {
        final GetIndexTemplatesResponse response = indices.getTemplates(
                indices.prepareGetTemplates(templateName).request()).get(30, TimeUnit.SECONDS);

        for (final IndexTemplateMetaData t : response.getIndexTemplates())
            if (t.getName().equals(templateName))
                return compareTemplate(t, templateName, mappings);

        return false;
    }

    private boolean compareTemplate(final IndexTemplateMetaData t, String templateName,
            Map<String, XContentBuilder> mappings) throws IOException {
        if (t.getTemplate() == null)
            return false;

        if (!t.getTemplate().equals(index.template()))
            return false;

        final ImmutableOpenMap<String, CompressedString> externalMappings = t.getMappings();

        if (externalMappings == null || externalMappings.isEmpty())
            return false;

        for (final Map.Entry<String, XContentBuilder> mapping : mappings.entrySet()) {
            final CompressedString external = externalMappings.get(mapping.getKey());

            if (external == null)
                return false;

            // This is a fairly dirty way, but ES seems to preserve the original document in verbatim, so it works for
            // now.
            if (!mapping.getValue().string().equals(external.string()))
                return false;
        }

        return true;
    }

    private void createTemplate(final IndicesAdminClient indices, String templateName,
            Map<String, XContentBuilder> mappings) throws Exception {
        final PutIndexTemplateRequestBuilder put = indices.preparePutTemplate(templateName);

        put.setTemplate(index.template());

        for (final Map.Entry<String, XContentBuilder> mapping : mappings.entrySet()) {
            put.addMapping(mapping.getKey(), mapping.getValue());
        }

        final PutIndexTemplateResponse response = put.get();

        if (!response.isAcknowledged())
            throw new Exception("Failed to setup mapping: " + response.toString());
    }

    private BulkProcessor configureBulkProcessor(final Client client) {
        final Builder builder = BulkProcessor.builder(client, new Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                reporter.reportWriteFailure(request.numberOfActions());
                log.error("Failed to write bulk", failure);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
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
};