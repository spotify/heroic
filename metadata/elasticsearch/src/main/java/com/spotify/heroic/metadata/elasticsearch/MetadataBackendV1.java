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

package com.spotify.heroic.metadata.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.AbstractElasticsearchMetadataBackend;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterModifier;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindSeriesIds;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

@ElasticsearchScope
@ToString(of = {"connection"})
public class MetadataBackendV1 extends AbstractElasticsearchMetadataBackend
    implements MetadataBackend, LifeCycles {

    private static String removed =
        "Support for Elasticsearch metadata schema 'v1' has been removed. Please use the much " +
            "improved 'kv' schema.";

    @Inject
    public MetadataBackendV1(
        Groups groups, MetadataBackendReporter reporter, AsyncFramework async,
        Managed<Connection> connection, RateLimitedCache<Pair<String, HashCode>> writeCache,
        FilterModifier modifier, @Named("configure") boolean configure
    ) {
        // Silence compiler
        super(async, removed);

        throw new RuntimeException(removed);
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        throw new RuntimeException(removed);
    }

    @Override
    protected Managed<Connection> connection() {
        throw new RuntimeException(removed);
    }

    @Override
    protected QueryBuilder filter(Filter filter) {
        throw new RuntimeException(removed);
    }

    @Override
    protected Series toSeries(SearchHit hit) {
        throw new RuntimeException(removed);
    }

    private String toId(SearchHit source) {
        throw new RuntimeException(removed);
    }

    @Override
    public AsyncFuture<Void> configure() {
        throw new RuntimeException(removed);
    }

    @Override
    public Groups groups() {
        throw new RuntimeException(removed);
    }

    @Override
    public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
        throw new RuntimeException(removed);
    }

    @Override
    public AsyncFuture<WriteMetadata> write(final WriteMetadata.Request request) {
        throw new RuntimeException(removed);
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
        throw new RuntimeException(removed);
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
        throw new RuntimeException(removed);
    }

    @Override
    public AsyncFuture<FindSeriesIds> findSeriesIds(final FindSeriesIds.Request request) {
        throw new RuntimeException(removed);
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
        throw new RuntimeException(removed);
    }

    private AsyncFuture<FindTagKeys> findTagKeys(final FindTagKeys.Request filter) {
        throw new RuntimeException(removed);
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
        throw new RuntimeException(removed);
    }

    @Override
    public boolean isReady() {
        return false;
    }

    @Data
    static class FindTagKeys {
        private final Set<String> keys;
        private final int size;

        @Data
        public static class Request {
            private final Filter filter;
            private final DateRange range;
            private final OptionalLimit limit;
        }
    }

    public static BackendType backendType() {
        final Map<String, Map<String, Object>> mappings = new HashMap<>();
        return new BackendType(mappings, ImmutableMap.of(), MetadataBackendV1.class);
    }
}
