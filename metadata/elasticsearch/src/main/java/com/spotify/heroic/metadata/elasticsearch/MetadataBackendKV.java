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
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.elasticsearch.AbstractElasticsearchMetadataBackend;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.RegexFilter;
import com.spotify.heroic.filter.StartsWithFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindSeriesIds;
import com.spotify.heroic.metadata.FindSeriesIdsStream;
import com.spotify.heroic.metadata.FindSeriesStream;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;
import eu.toolchain.async.Transform;
import lombok.ToString;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.matchAllFilter;
import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.prefixFilter;
import static org.elasticsearch.index.query.FilterBuilders.regexpFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;

@ElasticsearchScope
@ToString(of = {"connection"})
public class MetadataBackendKV extends AbstractElasticsearchMetadataBackend
    implements MetadataBackend, LifeCycles {
    public static final String WRITE_CACHE_SIZE = "write-cache-size";

    static final String KEY = "key";
    static final String TAGS = "tags";
    static final String TAG_KEYS = "tag_keys";
    static final Character TAG_DELIMITER = '\0';

    static final String TYPE_METADATA = "metadata";

    static final TimeValue SCROLL_TIME = TimeValue.timeValueMillis(5000);
    static final int SCROLL_SIZE = 1000;

    private final Groups groups;
    private final MetadataBackendReporter reporter;
    private final AsyncFramework async;
    private final Managed<Connection> connection;
    private final RateLimitedCache<Pair<String, HashCode>> writeCache;
    private final boolean configure;

    @Inject
    public MetadataBackendKV(
        Groups groups, MetadataBackendReporter reporter, AsyncFramework async,
        Managed<Connection> connection, RateLimitedCache<Pair<String, HashCode>> writeCache,
        @Named("configure") boolean configure
    ) {
        super(async, TYPE_METADATA);
        this.groups = groups;
        this.reporter = reporter;
        this.async = async;
        this.connection = connection;
        this.writeCache = writeCache;
        this.configure = configure;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    @Override
    protected Managed<Connection> connection() {
        return connection;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return doto(Connection::configure);
    }

    @Override
    public Groups groups() {
        return groups;
    }

    @Override
    public AsyncFuture<FindTags> findTags(final FindTags.Request filter) {
        return doto(c -> async.resolved(FindTags.EMPTY));
    }

    @Override
    public AsyncFuture<WriteMetadata> write(final WriteMetadata.Request request) {
        return doto(c -> {
            final Series series = request.getSeries();
            final String id = series.hash();

            final String[] indices;

            try {
                indices = c.writeIndices();
            } catch (NoIndexSelectedException e) {
                return async.failed(e);
            }

            final List<AsyncFuture<WriteMetadata>> writes = new ArrayList<>();

            for (final String index : indices) {
                if (!writeCache.acquire(Pair.of(index, series.getHashCode()))) {
                    reporter.reportWriteDroppedByRateLimit();
                    continue;
                }

                final XContentBuilder source = XContentFactory.jsonBuilder();

                source.startObject();
                buildContext(source, series);
                source.endObject();

                final IndexRequestBuilder builder = c
                    .index(index, TYPE_METADATA)
                    .setId(id)
                    .setSource(source)
                    .setOpType(OpType.CREATE);

                final RequestTimer<WriteMetadata> timer = WriteMetadata.timer();

                AsyncFuture<WriteMetadata> result =
                    bind(builder.execute()).directTransform(response -> timer.end());

                writes.add(result);
            }

            return async.collect(writes, WriteMetadata.reduce());
        });
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final CountSeries.Request filter) {
        return doto(c -> {
            final OptionalLimit limit = filter.getLimit();

            if (limit.isZero()) {
                return async.resolved(CountSeries.of());
            }

            final FilterBuilder f = filter(filter.getFilter());

            final CountRequestBuilder builder = c.count(TYPE_METADATA);
            limit.asInteger().ifPresent(builder::setTerminateAfter);

            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            return bind(builder.execute()).directTransform(
                response -> CountSeries.of(response.getCount(), false));
        });
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
        return entries(request.getFilter(), request.getLimit(), request.getRange(), this::toSeries,
            l -> FindSeries.of(l.getSet(), l.isLimited()), builder -> {
            });
    }

    @Override
    public AsyncObservable<FindSeriesStream> findSeriesStream(final FindSeries.Request request) {
        return entriesStream(request.getLimit(), request.getFilter(), request.getRange(),
            this::toSeries, FindSeriesStream::of, builder -> {
            });
    }

    @Override
    public AsyncFuture<FindSeriesIds> findSeriesIds(final FindSeriesIds.Request request) {
        return entries(request.getFilter(), request.getLimit(), request.getRange(), this::toId,
            l -> FindSeriesIds.of(l.getSet(), l.isLimited()), builder -> {
                builder.setFetchSource(false);
            });
    }

    @Override
    public AsyncObservable<FindSeriesIdsStream> findSeriesIdsStream(
        final FindSeriesIds.Request request
    ) {
        return entriesStream(request.getLimit(), request.getFilter(), request.getRange(),
            this::toId, FindSeriesIdsStream::of, builder -> {
                builder.setFetchSource(false);
            });
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
        return doto(c -> {
            final FilterBuilder f = filter(request.getFilter());

            final DeleteByQueryRequestBuilder builder =
                c.deleteByQuery(TYPE_METADATA);

            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            return bind(builder.execute()).directTransform(response -> DeleteSeries.of());
        });
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
        return doto(c -> {
            final FilterBuilder f = filter(request.getFilter());

            final SearchRequestBuilder builder =
                c.search(TYPE_METADATA).setSearchType("count");

            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            {
                final AggregationBuilder<?> terms =
                    AggregationBuilders.terms("terms").field(KEY).size(0);
                builder.addAggregation(terms);
            }

            return bind(builder.execute()).directTransform(response -> {
                final Terms terms = (Terms) response.getAggregations().get("terms");

                final Set<String> keys = new HashSet<String>();

                int size = terms.getBuckets().size();
                int duplicates = 0;

                for (final Terms.Bucket bucket : terms.getBuckets()) {
                    if (keys.add(bucket.getKey())) {
                        duplicates += 1;
                    }
                }

                return FindKeys.of(keys, size, duplicates);
            });
        });
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    protected Series toSeries(SearchHit hit) {
        final Map<String, Object> source = hit.getSource();
        final String key = (String) source.get(KEY);
        final Iterator<Map.Entry<String, String>> tags =
            ((List<String>) source.get(TAGS)).stream().map(this::buildTag).iterator();
        return Series.of(key, tags);
    }

    private <T, O> AsyncFuture<O> entries(
        final Filter filter, final OptionalLimit limit, final DateRange range,
        final Function<SearchHit, T> converter, final Transform<LimitedSet<T>, O> collector,
        final Consumer<SearchRequestBuilder> modifier
    ) {
        final FilterBuilder f = filter(filter);

        return doto(c -> {
            final SearchRequestBuilder builder = c
                .search(TYPE_METADATA)
                .setScroll(SCROLL_TIME)
                .setSearchType(SearchType.SCAN);

            builder.setSize(limit.asMaxInteger(SCROLL_SIZE));
            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            modifier.accept(builder);

            return scrollEntries(c, builder, limit, converter).directTransform(collector);
        });
    }

    private <T, O> AsyncObservable<O> entriesStream(
        final OptionalLimit limit, final Filter f, final DateRange range,
        final Function<SearchHit, T> converter, final Function<Set<T>, O> collector,
        final Consumer<SearchRequestBuilder> modifier
    ) {
        final FilterBuilder filter = filter(f);

        return observer -> connection.doto(c -> {
            final SearchRequestBuilder builder = c
                .search(TYPE_METADATA)
                .setScroll(SCROLL_TIME)
                .setSearchType(SearchType.SCAN);

            builder.setSize(limit.asMaxInteger(SCROLL_SIZE));
            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter));

            modifier.accept(builder);

            return bind(builder.execute()).lazyTransform((initial) -> {
                if (initial.getScrollId() == null) {
                    return async.resolved();
                }

                final String scrollId = initial.getScrollId();

                final Supplier<AsyncFuture<SearchResponse>> scroller =
                    () -> bind(c.prepareSearchScroll(scrollId).setScroll(SCROLL_TIME).execute());

                return scroller
                    .get()
                    .lazyTransform(new ScrollTransformStream<>(limit, scroller,
                        set -> observer.observe(collector.apply(set)), converter));
            });
        }).onDone(observer.onDone());
    }

    private String toId(SearchHit hit) {
        return hit.getId();
    }

    private <T> AsyncFuture<T> doto(ManagedAction<Connection, T> action) {
        return connection.doto(action);
    }

    AsyncFuture<Void> start() {
        final AsyncFuture<Void> future = connection.start();

        if (!configure) {
            return future;
        }

        return future.lazyTransform(v -> configure());
    }

    AsyncFuture<Void> stop() {
        return connection.stop();
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

    private static final Filter.Visitor<FilterBuilder> FILTER_CONVERTER =
        new Filter.Visitor<FilterBuilder>() {
            @Override
            public FilterBuilder visitTrue(final TrueFilter t) {
                return matchAllFilter();
            }

            @Override
            public FilterBuilder visitFalse(final FalseFilter f) {
                return notFilter(matchAllFilter());
            }

            @Override
            public FilterBuilder visitAnd(final AndFilter and) {
                return andFilter(convertTerms(and.terms()));
            }

            @Override
            public FilterBuilder visitOr(final OrFilter or) {
                return orFilter(convertTerms(or.terms()));
            }

            @Override
            public FilterBuilder visitNot(final NotFilter not) {
                return notFilter(not.getFilter().visit(this));
            }

            @Override
            public FilterBuilder visitMatchTag(final MatchTagFilter matchTag) {
                return termFilter(TAGS, matchTag.getTag() + TAG_DELIMITER + matchTag.getValue());
            }

            @Override
            public FilterBuilder visitStartsWith(final StartsWithFilter startsWith) {
                return prefixFilter(TAGS,
                    startsWith.getTag() + TAG_DELIMITER + startsWith.getValue());
            }

            @Override
            public FilterBuilder visitRegex(final RegexFilter regex) {
                return regexpFilter(TAGS,
                    regex.getTag() + TAG_DELIMITER + regex.getValue());
            }

            @Override
            public FilterBuilder visitHasTag(final HasTagFilter hasTag) {
                return termFilter(TAG_KEYS, hasTag.getTag());
            }

            @Override
            public FilterBuilder visitMatchKey(final MatchKeyFilter matchKey) {
                return termFilter(KEY, matchKey.getValue());
            }

            @Override
            public FilterBuilder defaultAction(final Filter filter) {
                throw new IllegalArgumentException("Unsupported filter statement: " + filter);
            }

            private FilterBuilder[] convertTerms(final List<Filter> terms) {
                final FilterBuilder[] filters = new FilterBuilder[terms.size()];
                int i = 0;

                for (final Filter stmt : terms) {
                    filters[i++] = stmt.visit(this);
                }

                return filters;
            }
        };

    @Override
    protected FilterBuilder filter(final Filter filter) {
        return filter.visit(FILTER_CONVERTER);
    }

    @Override
    public Statistics getStatistics() {
        return Statistics.of(WRITE_CACHE_SIZE, writeCache.size());
    }

    public static BackendType backendType() {
        final Map<String, Map<String, Object>> mappings = new HashMap<>();
        mappings.put("metadata", ElasticsearchMetadataUtils.loadJsonResource("kv/metadata.json"));
        return new BackendType(mappings, ImmutableMap.of(), MetadataBackendKV.class);
    }
}
