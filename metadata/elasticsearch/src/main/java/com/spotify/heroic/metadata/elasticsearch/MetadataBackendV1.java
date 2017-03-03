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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.AbstractElasticsearchMetadataBackend;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterModifier;
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
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.boolFilter;
import static org.elasticsearch.index.query.FilterBuilders.matchAllFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.FilterBuilders.prefixFilter;
import static org.elasticsearch.index.query.FilterBuilders.regexpFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;

@ElasticsearchScope
@ToString(of = {"connection"})
public class MetadataBackendV1 extends AbstractElasticsearchMetadataBackend
    implements MetadataBackend, LifeCycles {
    private static final TimeValue SCROLL_TIME = TimeValue.timeValueMillis(5000);
    private static final int SCROLL_SIZE = 1000;

    private final Groups groups;
    private final MetadataBackendReporter reporter;
    private final AsyncFramework async;
    private final Managed<Connection> connection;
    private final RateLimitedCache<Pair<String, HashCode>> writeCache;
    private final FilterModifier modifier;
    private final boolean configure;

    @Inject
    public MetadataBackendV1(
        Groups groups, MetadataBackendReporter reporter, AsyncFramework async,
        Managed<Connection> connection, RateLimitedCache<Pair<String, HashCode>> writeCache,
        FilterModifier modifier, @Named("configure") boolean configure
    ) {
        super(async, ElasticsearchUtils.TYPE_METADATA);

        this.groups = groups;
        this.reporter = reporter;
        this.async = async;
        this.connection = connection;
        this.writeCache = writeCache;
        this.modifier = modifier;
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
    protected FilterBuilder filter(Filter filter) {
        return CTX.filter(filter);
    }

    @Override
    protected Series toSeries(SearchHit hit) {
        return ElasticsearchUtils.toSeries(hit.getSource());
    }

    private String toId(SearchHit source) {
        return source.getId();
    }

    @Override
    public AsyncFuture<Void> configure() {
        return doto(c -> c.configure());
    }

    @Override
    public Groups groups() {
        return groups;
    }

    private static final ElasticsearchUtils.FilterContext CTX = ElasticsearchUtils.context();

    @Override
    public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
        return doto(c -> {
            final Callable<SearchRequestBuilder> setup =
                () -> c.search(ElasticsearchUtils.TYPE_METADATA);

            final FindTagKeys.Request findTagKeys =
                new FindTagKeys.Request(request.getFilter(), request.getRange(),
                    request.getLimit());

            return findTagKeys(findTagKeys).lazyTransform(
                new FindTagsTransformer(request.getFilter(), setup, CTX));
        });
    }

    @Override
    public AsyncFuture<WriteMetadata> write(final WriteMetadata.Request request) {
        return doto(c -> {
            final Series series = request.getSeries();
            final String id = series.hash();

            final String[] indices = c.writeIndices();

            final List<AsyncFuture<WriteMetadata>> futures = new ArrayList<>();

            for (final String index : indices) {
                if (!writeCache.acquire(Pair.of(index, series.getHashCode()))) {
                    reporter.reportWriteDroppedByRateLimit();
                    continue;
                }

                final XContentBuilder source = XContentFactory.jsonBuilder();

                source.startObject();
                ElasticsearchUtils.buildMetadataDoc(source, series);
                source.endObject();

                final IndexRequestBuilder builder = c
                    .index(index, ElasticsearchUtils.TYPE_METADATA)
                    .setId(id)
                    .setSource(source)
                    .setOpType(OpType.CREATE);

                final RequestTimer<WriteMetadata> timer = WriteMetadata.timer();
                futures.add(bind(builder.execute()).directTransform(result -> timer.end()));
            }

            return async.collect(futures, WriteMetadata.reduce());
        });
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
        return doto(c -> {
            final OptionalLimit limit = request.getLimit();

            final FilterBuilder f = CTX.filter(request.getFilter());

            final CountRequestBuilder builder =
                c.count(ElasticsearchUtils.TYPE_METADATA);
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
    public AsyncFuture<FindSeriesIds> findSeriesIds(final FindSeriesIds.Request request) {
        return entries(request.getFilter(), request.getLimit(), request.getRange(), this::toId,
            l -> FindSeriesIds.of(l.getSet(), l.isLimited()), builder -> {
                builder.setFetchSource(false);
            });
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
        return doto(c -> {
            final FilterBuilder f = CTX.filter(request.getFilter());

            final DeleteByQueryRequestBuilder builder =
                c.deleteByQuery(ElasticsearchUtils.TYPE_METADATA);

            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            return bind(builder.execute()).directTransform(response -> DeleteSeries.of());
        });
    }

    private AsyncFuture<FindTagKeys> findTagKeys(final FindTagKeys.Request filter) {
        return doto(c -> {
            final FilterBuilder f = CTX.filter(filter.getFilter());

            final SearchRequestBuilder builder = c
                .search(ElasticsearchUtils.TYPE_METADATA)
                .setSearchType("count");

            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            {
                final AggregationBuilder<?> terms =
                    AggregationBuilders.terms("terms").field(CTX.tagsKey()).size(0);
                final AggregationBuilder<?> nested =
                    AggregationBuilders.nested("nested").path(CTX.tags()).subAggregation(terms);
                builder.addAggregation(nested);
            }

            return bind(builder.execute()).directTransform(response -> {
                final Terms terms;

                {
                    final Aggregations aggregations = response.getAggregations();
                    final Nested attributes = aggregations.get("nested");
                    terms = attributes.getAggregations().get("terms");
                }

                final Set<String> keys = new HashSet<>();

                for (final Terms.Bucket bucket : terms.getBuckets()) {
                    keys.add(bucket.getKey());
                }

                return new FindTagKeys(keys, keys.size());
            });
        });
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
        return doto(c -> {
            final FilterBuilder f = CTX.filter(request.getFilter());

            final SearchRequestBuilder builder = c
                .search(ElasticsearchUtils.TYPE_METADATA)
                .setSearchType("count");

            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            {
                final AggregationBuilder<?> terms =
                    AggregationBuilders.terms("terms").field(CTX.seriesKey()).size(0);
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

    private AsyncFuture<Void> start() {
        final AsyncFuture<Void> future = connection.start();

        if (!configure) {
            return future;
        }

        return future.lazyTransform(v -> configure());
    }

    private <T, O> AsyncFuture<O> entries(
        final Filter filter, final OptionalLimit limit, final DateRange range,
        final Function<SearchHit, T> converter, final Transform<LimitedSet<T>, O> collector,
        final Consumer<SearchRequestBuilder> modifier
    ) {
        return doto(c -> {
            final FilterBuilder f = CTX.filter(filter);

            final SearchRequestBuilder builder = c
                .search(ElasticsearchUtils.TYPE_METADATA)
                .setScroll(SCROLL_TIME)
                .setSearchType(SearchType.SCAN);

            builder.setSize(limit.asMaxInteger(SCROLL_SIZE));
            builder.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            modifier.accept(builder);

            return scrollEntries(c, builder, limit, converter).directTransform(collector);
        });
    }

    private AsyncFuture<Void> stop() {
        return connection.stop();
    }

    private <T> AsyncFuture<T> doto(ManagedAction<Connection, T> action) {
        return connection.doto(action);
    }

    private static final class ElasticsearchUtils {
        public static final String TYPE_METADATA = "metadata";

        /**
         * Fields for type "metadata".
         */
        public static final String METADATA_KEY = "key";
        public static final String METADATA_TAGS = "tags";

        /**
         * common fields, but nested in different ways depending on document type.
         *
         * @see FilterContext
         */
        public static final String KEY = "key";
        public static final String TAGS = "tags";
        public static final String TAGS_KEY = "key";
        public static final String TAGS_KEY_RAW = "key.raw";
        public static final String TAGS_VALUE = "value";
        public static final String TAGS_VALUE_RAW = "value.raw";

        @SuppressWarnings("unchecked")
        public static Series toSeries(Map<String, Object> source) {
            final String key = (String) source.get("key");
            final SortedMap<String, String> tags =
                toTags((List<Map<String, String>>) source.get("tags"));
            return Series.of(key, tags);
        }

        public static SortedMap<String, String> toTags(final List<Map<String, String>> source) {
            final SortedMap<String, String> tags = new TreeMap<>();

            for (final Map<String, String> entry : source) {
                final String key = entry.get("key");
                final String value = entry.get("value");

                if (value != null && key != null) {
                    tags.put(key, value);
                }
            }

            return tags;
        }

        public static void buildMetadataDoc(final XContentBuilder b, Series series)
            throws IOException {
            b.field(METADATA_KEY, series.getKey());

            b.startArray(METADATA_TAGS);

            if (series.getTags() != null && !series.getTags().isEmpty()) {
                for (final Map.Entry<String, String> entry : series.getTags().entrySet()) {
                    b.startObject();
                    b.field(TAGS_KEY, entry.getKey());
                    b.field(TAGS_VALUE, entry.getValue());
                    b.endObject();
                }
            }

            b.endArray();
        }

        public static FilterContext context(String... path) {
            return new FilterContext(path);
        }

        public static final class FilterContext {
            private final String seriesKey;
            private final String tags;
            private final String tagsKey;
            private final String tagsValue;

            private FilterContext(String... path) {
                this(ImmutableList.<String>builder().add(path).build());
            }

            private FilterContext(List<String> path) {
                this.seriesKey = path(path, KEY);
                this.tags = path(path, TAGS);
                this.tagsKey = path(path, TAGS, TAGS_KEY_RAW);
                this.tagsValue = path(path, TAGS, TAGS_VALUE_RAW);
            }

            private String path(List<String> path, String tail) {
                return StringUtils.join(ImmutableList.builder().addAll(path).add(tail).build(),
                    '.');
            }

            private String path(List<String> path, String tailN, String tail) {
                return StringUtils.join(
                    ImmutableList.builder().addAll(path).add(tailN).add(tail).build(), '.');
            }

            public String seriesKey() {
                return seriesKey;
            }

            public String tags() {
                return tags;
            }

            public String tagsKey() {
                return tagsKey;
            }

            public String tagsValue() {
                return tagsValue;
            }

            public FilterBuilder filter(final Filter filter) {
                return filter.visit(new Filter.Visitor<FilterBuilder>() {
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
                        final List<FilterBuilder> filters = new ArrayList<>(and.terms().size());

                        for (final Filter stmt : and.terms()) {
                            filters.add(filter(stmt));
                        }

                        return andFilter(filters.toArray(new FilterBuilder[0]));
                    }

                    @Override
                    public FilterBuilder visitOr(final OrFilter or) {
                        final List<FilterBuilder> filters = new ArrayList<>(or.terms().size());

                        for (final Filter stmt : or.terms()) {
                            filters.add(filter(stmt));
                        }

                        return andFilter(filters.toArray(new FilterBuilder[0]));
                    }

                    @Override
                    public FilterBuilder visitNot(final NotFilter not) {
                        return notFilter(filter(not.getFilter()));
                    }

                    @Override
                    public FilterBuilder visitMatchTag(final MatchTagFilter matchTag) {
                        final BoolFilterBuilder nested = boolFilter();
                        nested.must(termFilter(tagsKey, matchTag.getTag()));
                        nested.must(termFilter(tagsValue, matchTag.getValue()));
                        return nestedFilter(tags, nested);
                    }

                    @Override
                    public FilterBuilder visitStartsWith(final StartsWithFilter startsWith) {
                        final BoolFilterBuilder nested = boolFilter();
                        nested.must(termFilter(tagsKey, startsWith.getTag()));
                        nested.must(prefixFilter(tagsValue, startsWith.getValue()));
                        return nestedFilter(tags, nested);
                    }

                    @Override
                    public FilterBuilder visitRegex(final RegexFilter regex) {
                        final BoolFilterBuilder nested = boolFilter();
                        nested.must(termFilter(tagsKey, regex.getTag()));
                        nested.must(regexpFilter(tagsValue, regex.getValue()));
                        return nestedFilter(tags, nested);
                    }

                    @Override
                    public FilterBuilder visitHasTag(final HasTagFilter hasTag) {
                        final TermFilterBuilder nested = termFilter(tagsKey, hasTag.getTag());
                        return nestedFilter(tags, nested);
                    }

                    @Override
                    public FilterBuilder visitMatchKey(final MatchKeyFilter matchKey) {
                        return termFilter(seriesKey, matchKey.getValue());
                    }

                    @Override
                    public FilterBuilder defaultAction(final Filter filter) {
                        throw new IllegalArgumentException(
                            "Unsupported filter statement: " + filter);
                    }
                });
            }
        }
    }

    public AsyncFuture<FindTags> findtags(
        final Callable<SearchRequestBuilder> setup, final ElasticsearchUtils.FilterContext ctx,
        final FilterBuilder filter, final String key
    ) throws Exception {
        final SearchRequestBuilder request = setup.call().setSearchType("count").setSize(0);

        request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter));

        {
            final TermsBuilder terms =
                AggregationBuilders.terms("terms").field(ctx.tagsValue()).size(0);
            final FilterAggregationBuilder filterAggregation = AggregationBuilders
                .filter("filter")
                .filter(termFilter(ctx.tagsKey(), key))
                .subAggregation(terms);
            final NestedBuilder nestedAggregation = AggregationBuilders
                .nested("nested")
                .path(ctx.tags())
                .subAggregation(filterAggregation);
            request.addAggregation(nestedAggregation);
        }

        return bind(request.execute()).directTransform(response -> {
            final Terms terms;

            /* IMPORTANT: has to be unwrapped with the correct type in the correct order as
             * specified above! */
            {
                final Aggregations aggregations = response.getAggregations();
                final Nested tags = aggregations.get("nested");
                final SingleBucketAggregation f = tags.getAggregations().get("filter");
                terms = f.getAggregations().get("terms");
            }

            final Set<String> values = new HashSet<String>();

            for (final Terms.Bucket bucket : terms.getBuckets()) {
                values.add(bucket.getKey());
            }

            final Map<String, Set<String>> result = new HashMap<String, Set<String>>();
            result.put(key, values);
            return FindTags.of(result, result.size());
        });
    }

    @RequiredArgsConstructor
    private class FindTagsTransformer implements LazyTransform<FindTagKeys, FindTags> {
        private final Filter filter;
        private final Callable<SearchRequestBuilder> setup;
        private final ElasticsearchUtils.FilterContext ctx;

        @Override
        public AsyncFuture<FindTags> transform(FindTagKeys result) throws Exception {
            final List<AsyncFuture<FindTags>> callbacks = new ArrayList<>();

            for (final String tag : result.getKeys()) {
                callbacks.add(findSingle(tag));
            }

            return async.collect(callbacks, FindTags.reduce());
        }

        /**
         * Finds a single set of tags, excluding any criteria for this specific set of tags.
         *
         * @throws Exception
         */
        private AsyncFuture<FindTags> findSingle(final String tag) throws Exception {
            final Filter filter = modifier.removeTag(this.filter, tag);
            final FilterBuilder f = ctx.filter(filter);
            return findtags(setup, ctx, f, tag);
        }
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
        mappings.put("metadata", ElasticsearchMetadataUtils.loadJsonResource("v1/metadata.json"));
        return new BackendType(mappings, ImmutableMap.of(), MetadataBackendV1.class);
    }
}
