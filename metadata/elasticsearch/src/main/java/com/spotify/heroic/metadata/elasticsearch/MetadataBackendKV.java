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

import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.matchAllFilter;
import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.prefixFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.AbstractElasticsearchMetadataBackend;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.BackendTypeFactory;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.RateLimitExceededException;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataEntry;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;
import lombok.ToString;

@ToString(of = { "connection" })
public class MetadataBackendKV extends AbstractElasticsearchMetadataBackend implements MetadataBackend, LifeCycle {
    static final String KEY = "key";
    static final String TAGS = "tags";
    static final String TAG_KEYS = "tag_keys";
    static final Character TAG_DELIMITER = '\0';

    static final String TYPE_METADATA = "metadata";

    static final TimeValue SCROLL_TIME = TimeValue.timeValueMillis(5000);
    static final int MAX_SIZE = 1000;

    static final int ENTREIES_SCAN_SIZE = 1000;
    static final TimeValue ENTRIES_TIMEOUT = TimeValue.timeValueSeconds(5);

    public static final String TEMPLATE_NAME = "heroic";

    private final Groups groups;
    private final LocalMetadataBackendReporter reporter;
    private final AsyncFramework async;
    private final Managed<Connection> connection;
    private final RateLimitedCache<Pair<String, Series>, AsyncFuture<WriteResult>> writeCache;

    @Inject
    public MetadataBackendKV(Groups groups, LocalMetadataBackendReporter reporter,
            AsyncFramework async, Managed<Connection> connection,
            RateLimitedCache<Pair<String, Series>, AsyncFuture<WriteResult>> writeCache) {
        super(async);
        this.groups = groups;
        this.reporter = reporter;
        this.async = async;
        this.connection = connection;
        this.writeCache = writeCache;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return doto(new ManagedAction<Connection, Void>() {
            @Override
            public AsyncFuture<Void> action(Connection reference) throws Exception {
                return reference.configure();
            }
        });
    }

    @Override
    public Groups getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<Void> start() {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() {
        return connection.stop();
    }

    @Override
    public AsyncFuture<FindTags> findTags(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, FindTags>() {
            @Override
            public AsyncFuture<FindTags> action(final Connection c) throws Exception {
                return async.resolved(FindTags.EMPTY).onDone(reporter.reportFindTags());
            }
        });
    }

    @Override
    public AsyncFuture<WriteResult> write(final Series series, final DateRange range) {
        return doto(new ManagedAction<Connection, WriteResult>() {
            @Override
            public AsyncFuture<WriteResult> action(final Connection c) throws Exception {
                final String id = Long.toHexString(series.hash());

                final String[] indices;

                try {
                    indices = c.writeIndices(range);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                final List<AsyncFuture<WriteResult>> writes = new ArrayList<>();

                for (final String index : indices) {
                    final Pair<String, Series> key = Pair.of(index, series);

                    final Callable<AsyncFuture<WriteResult>> loader = new Callable<AsyncFuture<WriteResult>>() {
                        @Override
                        public AsyncFuture<WriteResult> call() throws Exception {
                            final XContentBuilder source = XContentFactory.jsonBuilder();

                            source.startObject();
                            buildContext(source, series);
                            source.endObject();

                            final IndexRequestBuilder request = c.index(index, TYPE_METADATA).setId(id)
                                    .setSource(source).setOpType(OpType.CREATE);

                            final long start = System.nanoTime();
                            return bind(request.execute()).directTransform(response -> WriteResult.of(System.nanoTime() - start));
                        }
                    };

                    try {
                        writes.add(writeCache.get(key, loader));
                    } catch (ExecutionException e) {
                        return async.failed(e);
                    } catch (RateLimitExceededException e) {
                        reporter.reportWriteDroppedByRateLimit();
                        continue;
                    }
                }

                return async.collect(writes, WriteResult.merger());
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

                final FilterBuilder f = filter(filter.getFilter());

                if (f == null) {
                    return async.resolved(CountSeries.EMPTY);
                }

                final CountRequestBuilder request;

                try {
                    request = c.count(filter.getRange(), TYPE_METADATA).setTerminateAfter(
                            filter.getLimit());
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                return bind(request.execute()).directTransform(response -> new CountSeries(response.getCount(), false));
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

                final FilterBuilder f = filter(filter.getFilter());

                if (f == null)
                    return async.resolved(FindSeries.EMPTY);

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), TYPE_METADATA).setSize(Math.min(MAX_SIZE, filter.getLimit()))
                            .setScroll(SCROLL_TIME).setSearchType(SearchType.SCAN);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                return scrollOverSeries(c, request, filter.getLimit(), h -> buildSeries(h.getSource()))
                        .onDone(reporter.reportFindTimeSeries());
            }
        });
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, DeleteSeries>() {
            @Override
            public AsyncFuture<DeleteSeries> action(final Connection c) throws Exception {
                final FilterBuilder f = filter(filter.getFilter());

                if (f == null)
                    return async.resolved(DeleteSeries.EMPTY);

                final DeleteByQueryRequestBuilder request;

                try {
                    request = c.deleteByQuery(filter.getRange(), TYPE_METADATA);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                return bind(request.execute()).directTransform(response -> new DeleteSeries(0, 0));
            }
        });
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, FindKeys>() {
            @Override
            public AsyncFuture<FindKeys> action(final Connection c) throws Exception {
                final FilterBuilder f = filter(filter.getFilter());

                if (f == null)
                    return async.resolved(FindKeys.EMPTY);

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), TYPE_METADATA).setSearchType("count");
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                {
                    final AggregationBuilder<?> terms = AggregationBuilders.terms("terms").field(KEY)
                            .size(0);
                    request.addAggregation(terms);
                }

                return bind(request.execute()).directTransform(response -> {
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
                }).onDone(reporter.reportFindKeys());
            }
        });
    }

    @Override
    public Iterable<MetadataEntry> entries(Filter filter, DateRange range) {
        final FilterBuilder f = filter(filter);

        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f);

        final Borrowed<Connection> c = connection.borrow();

        if (!c.isValid())
            throw new IllegalStateException("connection is not available");

        final SearchRequestBuilder request;

        try {
            request = c.get().search(range, TYPE_METADATA).setSize(ENTREIES_SCAN_SIZE)
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
                            results.add(buildSeries(hit.getSource()));
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

    private Series buildSeries(Map<String, Object> source) {
        final String key = (String) source.get(KEY);
        final Iterator<Map.Entry<String, String>> tags = ((List<String>) source.get(TAGS)).stream().map(this::buildTag)
                .iterator();
        return Series.of(key, tags);
    }

    Map.Entry<String, String> buildTag(String kv) {
        final int index = kv.indexOf(TAG_DELIMITER);

        if (index == -1) {
            throw new IllegalArgumentException("invalid tag source: " + kv);
        }

        final String tk = kv.substring(0, index);
        final String tv = kv.substring(index + 1);
        return Pair.of(tk, tv);
    }

    static void buildContext(final XContentBuilder b, Series series) throws IOException {
        b.field(KEY, series.getKey());

        b.startArray(TAGS);

        for (final Map.Entry<String, String> entry : series.getTags().entrySet()) {
            b.value(entry.getKey() + TAG_DELIMITER + entry.getValue());
        }

        b.endArray();

        b.startArray(TAG_KEYS);

        for (final Map.Entry<String, String> entry : series.getTags().entrySet()) {
            b.value(entry.getKey());
        }

        b.endArray();
    }

    static FilterBuilder filter(final Filter filter) {
        if (filter instanceof Filter.True) {
            return matchAllFilter();
        }

        if (filter instanceof Filter.False) {
            return notFilter(matchAllFilter());
        }

        if (filter instanceof Filter.And) {
            final Filter.And and = (Filter.And) filter;
            final List<FilterBuilder> filters = new ArrayList<>(and.terms().size());

            for (final Filter stmt : and.terms())
                filters.add(filter(stmt));

            return andFilter(filters.toArray(new FilterBuilder[0]));
        }

        if (filter instanceof Filter.Or) {
            final Filter.Or or = (Filter.Or) filter;
            final List<FilterBuilder> filters = new ArrayList<>(or.terms().size());

            for (final Filter stmt : or.terms()) {
                filters.add(filter(stmt));
            }

            return orFilter(filters.toArray(new FilterBuilder[0]));
        }

        if (filter instanceof Filter.Not) {
            final Filter.Not not = (Filter.Not) filter;
            return notFilter(filter(not.first()));
        }

        if (filter instanceof Filter.MatchTag) {
            final Filter.MatchTag matchTag = (Filter.MatchTag) filter;
            return termFilter(TAGS, matchTag.first() + '\0' + matchTag.second());
        }

        if (filter instanceof Filter.StartsWith) {
            final Filter.StartsWith startsWith = (Filter.StartsWith) filter;
            return prefixFilter(TAGS, startsWith.first() + '\0' + startsWith.second());
        }

        if (filter instanceof Filter.Regex) {
            throw new IllegalArgumentException("regex not supported");
        }

        if (filter instanceof Filter.HasTag) {
            final Filter.HasTag hasTag = (Filter.HasTag) filter;
            return termFilter(TAG_KEYS, hasTag.first());
        }

        if (filter instanceof Filter.MatchKey) {
            final Filter.MatchKey matchKey = (Filter.MatchKey) filter;
            return termFilter(KEY, matchKey.first());
        }

        throw new IllegalArgumentException("Invalid filter statement: " + filter);
    }

    public static BackendTypeFactory<ElasticsearchMetadataModule, MetadataBackend> factory() {
        return new BackendTypeFactory<ElasticsearchMetadataModule, MetadataBackend>() {
            @Override
            public BackendType<MetadataBackend> setup(final ElasticsearchMetadataModule module) {
                return new BackendType<MetadataBackend>() {
                    @Override
                    public Map<String, Map<String, Object>> mappings() throws IOException {
                        final Map<String, Map<String, Object>> mappings = new HashMap<>();
                        mappings.put("metadata", ElasticsearchMetadataUtils.loadJsonResource("kv/metadata.json"));
                        return mappings;
                    }

                    @Override
                    public Map<String, Object> settings() throws IOException {
                        return ImmutableMap.of();
                    }

                    @Override
                    public Class<? extends MetadataBackend> type() {
                        return MetadataBackendKV.class;
                    }
                };
            }
        };
    }
}
