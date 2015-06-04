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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.ToString;

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.ElasticsearchUtils;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterModifier;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.elasticsearch.async.FindTagsTransformer;
import com.spotify.heroic.metadata.model.CountSeries;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTagKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.MetadataEntry;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;

@ToString(of = { "connection" })
public class ElasticsearchMetadataBackend implements MetadataBackend, LifeCycle {
    private static final TimeValue TIMEOUT = TimeValue.timeValueMillis(10000);
    private static final TimeValue SCROLL_TIME = TimeValue.timeValueMillis(5000);
    private static final int MAX_SIZE = 1000;

    public static final String TEMPLATE_NAME = "heroic";

    @Inject
    @Named("groups")
    private Set<String> groups;

    @Inject
    private ReadWriteThreadPools pools;

    @Inject
    private LocalMetadataBackendReporter reporter;

    @Inject
    private FilterModifier modifier;

    @Inject
    private AsyncFramework async;

    @Inject
    private Managed<Connection> connection;

    @Override
    public Set<String> getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<Void> start() throws Exception {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
        return connection.stop();
    }

    private final ElasticsearchUtils.FilterContext CTX = ElasticsearchUtils.context();

    /**
     * prevent unnecessary writes if entry is already in cache. Integer is the hashCode of the series.
     */
    private final Cache<Pair<String, Series>, Boolean> writeCache = CacheBuilder.newBuilder().concurrencyLevel(4)
            .expireAfterWrite(30, TimeUnit.MINUTES).build();

    @Override
    public AsyncFuture<FindTags> findTags(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, FindTags>() {
            @Override
            public AsyncFuture<FindTags> action(final Connection c) throws Exception {
                final Callable<SearchRequestBuilder> setup = new Callable<SearchRequestBuilder>() {
                    @Override
                    public SearchRequestBuilder call() throws Exception {
                        return c.search(filter.getRange(), ElasticsearchUtils.TYPE_METADATA);
                    }
                };

                return findTagKeys(filter).transform(
                        new FindTagsTransformer(async, pools.read(), modifier, filter.getFilter(), setup, CTX)).onAny(
                        reporter.reportFindTags());
            }
        });
    }

    @Override
    public AsyncFuture<WriteResult> write(final Series series, final DateRange range) {
        return doto(new ManagedAction<Connection, WriteResult>() {
            @Override
            public AsyncFuture<WriteResult> action(final Connection c) throws Exception {
                final BulkProcessor bulk = c.bulk();

                final String id = Integer.toHexString(series.hashCode());

                final String[] indices;

                try {
                    indices = c.indices(range);
                } catch (NoIndexSelectedException e) {
                    return async.cancelled();
                }

                for (final String index : indices) {
                    final Pair<String, Series> key = Pair.of(index, series);

                    final Callable<Boolean> loader = new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            final XContentBuilder source = XContentFactory.jsonBuilder();

                            source.startObject();
                            ElasticsearchUtils.buildMetadataDoc(source, series);
                            source.endObject();

                            bulk.add(new IndexRequest(index, ElasticsearchUtils.TYPE_METADATA, id).source(source)
                                    .opType(OpType.CREATE));
                            return true;
                        }
                    };

                    writeCache.get(key, loader);
                }

                return async.resolved(WriteResult.of(0));
            }
        });
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, CountSeries>() {
            @Override
            public AsyncFuture<CountSeries> action(final Connection c) throws Exception {
                if (filter.getLimit() <= 0) {
                    return async.resolved(CountSeries.EMPTY);
                }

                final FilterBuilder f = CTX.filter(filter.getFilter());

                if (f == null) {
                    return async.resolved(CountSeries.EMPTY);
                }

                final CountRequestBuilder request;

                try {
                    request = c.count(filter.getRange(), ElasticsearchUtils.TYPE_METADATA).setTerminateAfter(
                            filter.getLimit());
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                return async.call(new Callable<CountSeries>() {
                    @Override
                    public CountSeries call() throws Exception {
                        final CountResponse response = request.get(TIMEOUT);
                        return new CountSeries(response.getCount(), false);
                    }
                }, pools.read()).onAny(reporter.reportCountSeries());
            }
        });
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, FindSeries>() {
            @Override
            public AsyncFuture<FindSeries> action(final Connection c) throws Exception {
                if (filter.getLimit() <= 0)
                    return async.resolved(FindSeries.EMPTY);

                final FilterBuilder f = CTX.filter(filter.getFilter());

                if (f == null)
                    return async.resolved(FindSeries.EMPTY);

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), ElasticsearchUtils.TYPE_METADATA)
                            .setSize(Math.min(MAX_SIZE, filter.getLimit())).setScroll(TIMEOUT)
                            .setSearchType(SearchType.SCAN);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                final Set<Series> series = new HashSet<Series>();

                return async.call(new Callable<FindSeries>() {
                    @Override
                    public FindSeries call() throws Exception {
                        final SearchResponse response = request.get(TIMEOUT);

                        String currentId = response.getScrollId();

                        int size = 0;

                        do {
                            final SearchResponse resp = c.prepareSearchScroll(currentId).setScroll(SCROLL_TIME)
                                    .get(TIMEOUT);

                            currentId = resp.getScrollId();

                            final SearchHit[] hits = resp.getHits().hits();

                            if (hits.length == 0) {
                                break;
                            }

                            for (final SearchHit hit : hits)
                                series.add(ElasticsearchUtils.toSeries(hit.getSource()));

                            size += hits.length;
                        } while (size < filter.getLimit());

                        return new FindSeries(series, series.size(), size - series.size());
                    }

                }, pools.read()).onAny(reporter.reportFindTimeSeries());
            }
        });
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, DeleteSeries>() {
            @Override
            public AsyncFuture<DeleteSeries> action(final Connection c) throws Exception {
                final FilterBuilder f = CTX.filter(filter.getFilter());

                if (f == null)
                    return async.resolved(DeleteSeries.EMPTY);

                final DeleteByQueryRequestBuilder request;

                try {
                    request = c.deleteByQuery(filter.getRange(), ElasticsearchUtils.TYPE_METADATA);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                return async.call(new Callable<DeleteSeries>() {
                    @Override
                    public DeleteSeries call() throws Exception {
                        request.execute().get();
                        return new DeleteSeries(0, 0);
                    }
                }, pools.write());
            }
        });
    }

    private AsyncFuture<FindTagKeys> findTagKeys(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, FindTagKeys>() {
            @Override
            public AsyncFuture<FindTagKeys> action(final Connection c) throws Exception {
                final FilterBuilder f = CTX.filter(filter.getFilter());

                if (f == null)
                    return async.resolved(FindTagKeys.EMPTY);

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), ElasticsearchUtils.TYPE_METADATA).setSearchType("count");
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                {
                    final AggregationBuilder<?> terms = AggregationBuilders.terms("terms").field(CTX.tagsKey()).size(0);
                    final AggregationBuilder<?> nested = AggregationBuilders.nested("nested").path(CTX.tags())
                            .subAggregation(terms);
                    request.addAggregation(nested);
                }

                return async.call(new Callable<FindTagKeys>() {
                    @Override
                    public FindTagKeys call() throws Exception {
                        final SearchResponse response = request.get(TIMEOUT);

                        final Terms terms;

                        {
                            final Aggregations aggregations = response.getAggregations();
                            final Nested attributes = (Nested) aggregations.get("nested");
                            terms = (Terms) attributes.getAggregations().get("terms");
                        }

                        final Set<String> keys = new HashSet<String>();

                        for (final Terms.Bucket bucket : terms.getBuckets()) {
                            keys.add(bucket.getKey());
                        }

                        return new FindTagKeys(keys, keys.size());
                    }
                }, pools.read()).on(reporter.reportFindTagKeys());
            }
        });
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, FindKeys>() {
            @Override
            public AsyncFuture<FindKeys> action(final Connection c) throws Exception {
                final FilterBuilder f = CTX.filter(filter.getFilter());

                if (f == null)
                    return async.resolved(FindKeys.EMPTY);

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), ElasticsearchUtils.TYPE_METADATA).setSearchType("count");
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                {
                    final AggregationBuilder<?> terms = AggregationBuilders.terms("terms").field(CTX.seriesKey())
                            .size(0);
                    request.addAggregation(terms);
                }

                return async.call(new Callable<FindKeys>() {
                    @Override
                    public FindKeys call() throws Exception {
                        final SearchResponse response = request.get();

                        final Terms terms = (Terms) response.getAggregations().get("terms");

                        final Set<String> keys = new HashSet<String>();

                        int size = terms.getBuckets().size();
                        int duplicates = 0;

                        for (final Terms.Bucket bucket : terms.getBuckets()) {
                            if (keys.add(bucket.getKey())) {
                                duplicates += 1;
                            }
                        }

                        return new FindKeys(keys, size, duplicates);
                    }

                }, pools.read()).on(reporter.reportFindKeys());
            }
        });
    }

    private static final int ENTREIES_SCAN_SIZE = 1000;
    private static final TimeValue ENTRIES_TIMEOUT = TimeValue.timeValueSeconds(5);

    @Override
    public Iterable<MetadataEntry> entries(Filter filter, DateRange range) {
        final FilterBuilder f = CTX.filter(filter);

        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f);

        final Borrowed<Connection> c = connection.borrow();

        if (!c.isValid())
            throw new IllegalStateException("connection is not available");

        final SearchRequestBuilder request;

        try {
            request = c.get().search(range, ElasticsearchUtils.TYPE_METADATA).setSize(ENTREIES_SCAN_SIZE)
                    .setScroll(ENTRIES_TIMEOUT).setSearchType(SearchType.SCAN).setQuery(query);
        } catch (NoIndexSelectedException e) {
            throw new IllegalArgumentException("no valid index selected", e);
        }

        return new Iterable<MetadataEntry>() {
            @Override
            public Iterator<MetadataEntry> iterator() {
                final SearchResponse response = request.get(ENTRIES_TIMEOUT);

                return new Iterator<MetadataEntry>() {
                    private LinkedList<Series> current;
                    private String currentId = response.getScrollId();

                    @Override
                    public boolean hasNext() {
                        if (current == null || current.isEmpty()) {
                            current = loadNext();
                        }

                        final boolean next = current != null && !current.isEmpty();

                        if (!next)
                            c.release();

                        return next;
                    }

                    private LinkedList<Series> loadNext() {
                        final SearchResponse resp = c.get().prepareSearchScroll(currentId).setScroll(ENTRIES_TIMEOUT)
                                .get();

                        currentId = resp.getScrollId();

                        final SearchHit[] hits = resp.getHits().hits();

                        final LinkedList<Series> results = new LinkedList<>();

                        for (final SearchHit hit : hits) {
                            results.add(ElasticsearchUtils.toSeries(hit.getSource()));
                        }

                        return results;
                    }

                    @Override
                    public MetadataEntry next() {
                        if (current == null || current.isEmpty()) {
                            throw new IllegalStateException("no entries available");
                        }

                        return new MetadataEntry(current.poll());
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
        };
    }

    @Override
    public AsyncFuture<Void> refresh() {
        return async.resolved(null);
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    private <T> AsyncFuture<T> doto(ManagedAction<Connection, T> action) {
        return connection.doto(action);
    }
}
