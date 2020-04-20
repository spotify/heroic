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

import com.spotify.heroic.elasticsearch.index.IndexMapping;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import kotlin.Pair;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection abstraction for TransportClient.
 */
public class TransportConnection implements Connection<StringTerms> {
    private static final Logger log = LoggerFactory.getLogger(TransportClientWrapper.class);
    private final AsyncFramework async;
    private final IndexMapping index;
    private final TransportClient client;

    private final String templateName;
    private final BackendType type;

    TransportConnection(
        final AsyncFramework async,
        final IndexMapping index,
        final String templateName,
        final BackendType type,
        final TransportClient client
    ) {
        this.async = async;
        this.index = index;
        this.templateName = templateName;
        this.type = type;
        this.client = client;
    }

    public String toString() {
        return "Connection(index=" + this.index + ", clientWrapper=" + this.client + ")";
    }

    @NotNull
    @Override
    public IndexMapping getIndex() {
        return index;
    }

    @Override
    @NotNull
    public AsyncFuture<Void> close() {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        futures.add(async.call(() -> {
            client.close();
            return null;
        }));

        return async.collectAndDiscard(futures);
    }

    @Override
    @NotNull
    public AsyncFuture<Void> configure() {
        final IndicesAdminClient indices = client.admin().indices();

        final List<AsyncFuture<AcknowledgedResponse>> writes = new ArrayList<>();

        // ES 7+ no longer allows indexes to have multiple types. Each type is now it's own index.
        for (final Map.Entry<String, Map<String, Object>> mapping : type.getMappings().entrySet()) {
            final String indexType = mapping.getKey();
            final String templateWithType = templateName + "-" + indexType;
            final String pattern = index.getTemplate().replaceAll("\\*", indexType + "-*");

            log.info("[{}] updating template for {}", templateWithType, pattern);

            Map<String, Object> settings = new HashMap<>(type.getSettings());
            settings.put("index", index.getSettings());

            final PutIndexTemplateRequestBuilder put = indices.preparePutTemplate(templateWithType)
                .setSettings(settings)
                .setPatterns(List.of(pattern))
                .addMapping("_doc", mapping.getValue())
                .setOrder(100);

            final ResolvableFuture<AcknowledgedResponse> future = async.future();
            writes.add(future);
            put.execute(new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (!response.isAcknowledged()) {
                        future.fail(new Exception("request not acknowledged"));
                        return;
                    }
                    future.resolve(null);
                }

                @Override
                public void onFailure(Exception e) {
                    future.fail(e);
                }
            });
        }

        return async.collectAndDiscard(writes);
    }

    @Override
    public void searchScroll(
        @NotNull String scrollId,
        @NotNull TimeValue timeout,
        @NotNull ActionListener<SearchResponse> listener
    ) {
        SearchScrollRequest request = new SearchScrollRequest(scrollId).scroll(timeout);
        client.searchScroll(request, listener);
    }

    @Override
    @NotNull
    public ActionFuture<ClearScrollResponse> clearSearchScroll(@NotNull String scrollId) {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);
        return client.clearScroll(request);
    }

    @Override
    public void execute(
        @NotNull SearchRequest request,
        @NotNull ActionListener<SearchResponse> listener
    ) {
        client.search(request, listener);
    }

    @NotNull
    @Override
    public SearchResponse execute(@NotNull SearchRequest request) {
        return client.search(request).actionGet();
    }

    @Override
    public void execute(
        @NotNull DeleteRequest request,
        @NotNull ActionListener<DeleteResponse> listener
    ) {
        client.delete(request, listener);
    }

    @Override
    public void execute(
        @NotNull BulkRequest request,
        @NotNull ActionListener<BulkResponse> listener
    ) {
        client.bulk(request, listener);
    }

    @Override
    public void execute(
        @NotNull IndexRequest request,
        @NotNull ActionListener<IndexResponse> listener
    ) {
        client.index(request, listener);
    }

    @NotNull
    @Override
    public Stream<Pair<String, SearchHits>> parseHits(@NotNull StringTerms terms) {
        return terms.getBuckets()
            .stream()
            .map(bucket -> {
                TopHits hits = bucket.getAggregations().get("hits");
                return new Pair<>(bucket.getKeyAsString(), hits.getHits());
            });
    }
}
