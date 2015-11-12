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

package com.spotify.heroic.suggest.elasticsearch;

import static com.spotify.heroic.suggest.elasticsearch.ElasticsearchSuggestUtils.loadJsonResource;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.boolFilter;
import static org.elasticsearch.index.query.FilterBuilders.matchAllFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.prefixFilter;
import static org.elasticsearch.index.query.FilterBuilders.regexpFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.BackendTypeFactory;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.RateLimitExceededException;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagSuggest.Suggestion;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import lombok.ToString;

@ToString(of = {"connection"})
public class SuggestBackendV1 implements SuggestBackend, LifeCycle, Grouped {
    private static final StandardAnalyzer analyzer = new StandardAnalyzer();
    public static final TimeValue TIMEOUT = TimeValue.timeValueMillis(10000);

    // different locations for the series used in filtering.
    private static final Utils.FilterContext SERIES_CTX = Utils.context();
    private static final Utils.FilterContext TAG_CTX = Utils.context(Utils.TAG_SERIES);

    private static final String[] KEY_SUGGEST_SOURCES = new String[] {Utils.SERIES_KEY_RAW};

    private static final String[] TAG_SUGGEST_SOURCES =
            new String[] {Utils.TAG_KEY, Utils.TAG_VALUE};

    private final AsyncFramework async;
    private final Managed<Connection> connection;
    private final LocalMetadataBackendReporter reporter;
    /**
     * prevent unnecessary writes if entry is already in cache. Integer is the hashCode of the
     * series.
     */
    private final RateLimitedCache<Pair<String, Series>, AsyncFuture<WriteResult>> writeCache;
    private final Groups groups;
    private final boolean configure;

    @Inject
    public SuggestBackendV1(final AsyncFramework async, final Managed<Connection> connection,
            final LocalMetadataBackendReporter reporter,
            final RateLimitedCache<Pair<String, Series>, AsyncFuture<WriteResult>> writeCache,
            final Groups groups, @Named("configure") boolean configure) {
        this.async = async;
        this.connection = connection;
        this.reporter = reporter;
        this.writeCache = writeCache;
        this.groups = groups;
        this.configure = configure;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return doto(c -> c.configure());
    }

    @Override
    public AsyncFuture<Void> start() {
        final AsyncFuture<Void> future = connection.start();

        if (!configure) {
            return future;
        }

        return future.lazyTransform(v -> configure());
    }

    @Override
    public AsyncFuture<Void> stop() {
        return connection.stop();
    }

    @Override
    public Groups getGroups() {
        return groups;
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    private <R> AsyncFuture<R> doto(ManagedAction<Connection, R> action) {
        return connection.doto(action);
    }

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(final RangeFilter filter,
            final List<String> exclude, final int groupLimit) {
        return doto(new ManagedAction<Connection, TagValuesSuggest>() {
            @Override
            public AsyncFuture<TagValuesSuggest> action(final Connection c) throws Exception {
                final FilterBuilder f = TAG_CTX.filter(filter.getFilter());

                final BoolQueryBuilder root = QueryBuilders.boolQuery();
                root.must(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                if (!exclude.isEmpty()) {
                    for (final String e : exclude) {
                        root.mustNot(QueryBuilders.matchQuery(Utils.TAG_KEY_RAW, e));
                    }
                }

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), Utils.TYPE_TAG)
                            .setSearchType(SearchType.COUNT).setQuery(root);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                {
                    final TermsBuilder terms = AggregationBuilders.terms("keys")
                            .field(Utils.TAG_KEY_RAW).size(filter.getLimit() + 1);
                    request.addAggregation(terms);
                    // make value bucket one entry larger than necessary to figure out when limiting
                    // is applied.
                    final TermsBuilder cardinality = AggregationBuilders.terms("values")
                            .field(Utils.TAG_VALUE_RAW).size(groupLimit + 1);
                    terms.subAggregation(cardinality);
                }

                return bind(request.execute(), new Transform<SearchResponse, TagValuesSuggest>() {
                    @Override
                    public TagValuesSuggest transform(SearchResponse response) throws Exception {
                        final List<TagValuesSuggest.Suggestion> suggestions = new ArrayList<>();

                        final Terms terms = (Terms) response.getAggregations().get("keys");

                        final List<Bucket> suggestionBuckets = terms.getBuckets();

                        for (final Terms.Bucket bucket : suggestionBuckets.subList(0,
                                Math.min(suggestionBuckets.size(), filter.getLimit()))) {
                            final Terms valueTerms = bucket.getAggregations().get("values");

                            final List<Bucket> valueBuckets = valueTerms.getBuckets();

                            final SortedSet<String> result = new TreeSet<>();

                            for (final Terms.Bucket valueBucket : valueBuckets) {
                                result.add(valueBucket.getKey());
                            }

                            final boolean limited = valueBuckets.size() > groupLimit;

                            final ImmutableList<String> values = ImmutableList.copyOf(result)
                                    .subList(0, Math.min(groupLimit, result.size()));

                            suggestions.add(new TagValuesSuggest.Suggestion(bucket.getKey(), values,
                                    limited));
                        }

                        return new TagValuesSuggest(new ArrayList<>(suggestions),
                                suggestionBuckets.size() > filter.getLimit());
                    }
                });
            }
        });
    }

    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(final RangeFilter filter,
            final String key) {
        return doto(new ManagedAction<Connection, TagValueSuggest>() {
            @Override
            public AsyncFuture<TagValueSuggest> action(final Connection c) throws Exception {
                final BoolQueryBuilder root = QueryBuilders.boolQuery();

                if (key != null && !key.isEmpty()) {
                    root.must(QueryBuilders.termQuery(Utils.TAG_KEY_RAW, key));
                }

                root.must(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),
                        TAG_CTX.filter(filter.getFilter())));

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), Utils.TYPE_TAG)
                            .setSearchType(SearchType.COUNT).setQuery(root);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                {
                    final TermsBuilder terms =
                            AggregationBuilders.terms("values").field(Utils.TAG_VALUE_RAW)
                                    .size(filter.getLimit() + 1).order(Order.term(true));
                    request.addAggregation(terms);
                }

                return bind(request.execute(), new Transform<SearchResponse, TagValueSuggest>() {
                    @Override
                    public TagValueSuggest transform(SearchResponse response) throws Exception {
                        final List<String> suggestions = new ArrayList<>();

                        final Terms terms = (Terms) response.getAggregations().get("values");

                        final List<Bucket> all = terms.getBuckets();

                        final List<Bucket> buckets =
                                all.subList(0, Math.min(all.size(), filter.getLimit()));

                        for (final Terms.Bucket bucket : buckets) {
                            suggestions.add(bucket.getKey());
                        }

                        boolean limited = all.size() > filter.getLimit();
                        return new TagValueSuggest(new ArrayList<>(suggestions), limited);
                    }
                });
            }
        });
    }

    @Override
    public AsyncFuture<TagKeyCount> tagKeyCount(final RangeFilter filter) {
        return doto(new ManagedAction<Connection, TagKeyCount>() {
            @Override
            public AsyncFuture<TagKeyCount> action(final Connection c) throws Exception {
                final FilterBuilder f = TAG_CTX.filter(filter.getFilter());

                final BoolQueryBuilder root = QueryBuilders.boolQuery();
                root.must(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), Utils.TYPE_TAG)
                            .setSearchType(SearchType.COUNT).setQuery(root);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                {
                    final TermsBuilder terms = AggregationBuilders.terms("keys")
                            .field(Utils.TAG_KEY_RAW).size(filter.getLimit());
                    request.addAggregation(terms);
                    final CardinalityBuilder cardinality = AggregationBuilders
                            .cardinality("cardinality").field(Utils.TAG_VALUE_RAW);
                    terms.subAggregation(cardinality);
                }

                return bind(request.execute(), new Transform<SearchResponse, TagKeyCount>() {
                    @Override
                    public TagKeyCount transform(SearchResponse response) throws Exception {
                        final Set<TagKeyCount.Suggestion> suggestions = new LinkedHashSet<>();

                        final Terms terms = (Terms) response.getAggregations().get("keys");

                        for (final Terms.Bucket bucket : terms.getBuckets()) {
                            final Cardinality cardinality =
                                    bucket.getAggregations().get("cardinality");
                            suggestions.add(new TagKeyCount.Suggestion(bucket.getKey(),
                                    cardinality.getValue()));
                        }

                        return new TagKeyCount(new ArrayList<>(suggestions), false);
                    }
                });
            }
        });
    }

    @Override
    public AsyncFuture<TagSuggest> tagSuggest(final RangeFilter filter, final MatchOptions options,
            final String key, final String value) {
        return doto(new ManagedAction<Connection, TagSuggest>() {
            @Override
            public AsyncFuture<TagSuggest> action(final Connection c) throws Exception {
                final QueryBuilder query;

                final BoolQueryBuilder fuzzy = QueryBuilders.boolQuery();

                if (key != null && !key.isEmpty()) {
                    try {
                        fuzzy.should(match(Utils.TAG_KEY, key, options));
                    } catch (IOException e) {
                        return async.failed(e);
                    }
                }

                if (value != null && !value.isEmpty()) {
                    try {
                        fuzzy.should(match(Utils.TAG_VALUE, value, options));
                    } catch (IOException e) {
                        return async.failed(e);
                    }
                }

                if (filter.getFilter() instanceof Filter.True) {
                    query = fuzzy;
                } else {
                    query = QueryBuilders.filteredQuery(fuzzy, TAG_CTX.filter(filter.getFilter()));
                }

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), Utils.TYPE_TAG)
                            .setSearchType(SearchType.COUNT).setQuery(query);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                // aggregation
                {
                    final MaxBuilder topHit = AggregationBuilders.max("topHit").script("_score");
                    final TopHitsBuilder hits = AggregationBuilders.topHits("hits").setSize(1)
                            .setFetchSource(TAG_SUGGEST_SOURCES, new String[0]);

                    final TermsBuilder kvs = AggregationBuilders.terms("kvs").field(Utils.TAG_KV)
                            .size(filter.getLimit()).order(Order.aggregation("topHit", false))
                            .subAggregation(hits).subAggregation(topHit);

                    request.addAggregation(kvs);
                }

                return bind(request.execute(), new Transform<SearchResponse, TagSuggest>() {
                    @Override
                    public TagSuggest transform(SearchResponse response) throws Exception {
                        final Set<Suggestion> suggestions = new LinkedHashSet<>();

                        final StringTerms kvs = (StringTerms) response.getAggregations().get("kvs");

                        for (final Terms.Bucket bucket : kvs.getBuckets()) {
                            final TopHits topHits = (TopHits) bucket.getAggregations().get("hits");
                            final SearchHits hits = topHits.getHits();
                            final SearchHit hit = hits.getAt(0);
                            final Map<String, Object> doc = hit.getSource();

                            final String key = (String) doc.get(Utils.TAG_KEY);
                            final String value = (String) doc.get(Utils.TAG_VALUE);
                            suggestions.add(new Suggestion(hits.getMaxScore(), key, value));
                        }

                        return new TagSuggest(new ArrayList<>(suggestions));
                    }
                });
            }
        });
    }

    @Override
    public AsyncFuture<KeySuggest> keySuggest(final RangeFilter filter, final MatchOptions options,
            final String key) {
        return doto(new ManagedAction<Connection, KeySuggest>() {
            @Override
            public AsyncFuture<KeySuggest> action(final Connection c) throws Exception {
                final QueryBuilder query;

                final BoolQueryBuilder fuzzy = QueryBuilders.boolQuery();

                if (key != null && !key.isEmpty()) {
                    try {
                        fuzzy.should(match(Utils.SERIES_KEY, key, options));
                    } catch (IOException e) {
                        return async.failed(e);
                    }
                }

                if (filter instanceof Filter.True) {
                    query = fuzzy;
                } else {
                    query = QueryBuilders.filteredQuery(fuzzy,
                            SERIES_CTX.filter(filter.getFilter()));
                }

                final SearchRequestBuilder request;

                try {
                    request = c.search(filter.getRange(), Utils.TYPE_SERIES)
                            .setSearchType(SearchType.COUNT).setQuery(query);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                // aggregation
                {
                    final MaxBuilder topHit = AggregationBuilders.max("top_hit").script("_score");
                    final TopHitsBuilder hits = AggregationBuilders.topHits("hits").setSize(1)
                            .setFetchSource(KEY_SUGGEST_SOURCES, new String[0]);

                    final TermsBuilder keys = AggregationBuilders.terms("keys")
                            .field(Utils.SERIES_KEY_RAW).size(filter.getLimit())
                            .order(Order.aggregation("top_hit", false)).subAggregation(hits)
                            .subAggregation(topHit);

                    request.addAggregation(keys);
                }

                return bind(request.execute(), new Transform<SearchResponse, KeySuggest>() {
                    @Override
                    public KeySuggest transform(SearchResponse response) throws Exception {
                        final Set<KeySuggest.Suggestion> suggestions = new LinkedHashSet<>();

                        final StringTerms keys =
                                (StringTerms) response.getAggregations().get("keys");

                        for (final Terms.Bucket bucket : keys.getBuckets()) {
                            final TopHits topHits = (TopHits) bucket.getAggregations().get("hits");
                            final SearchHits hits = topHits.getHits();
                            suggestions.add(
                                    new KeySuggest.Suggestion(hits.getMaxScore(), bucket.getKey()));
                        }

                        return new KeySuggest(new ArrayList<>(suggestions));
                    }
                });
            }
        });
    }

    @Override
    public AsyncFuture<WriteResult> write(final Series series, final DateRange range) {
        try (final Borrowed<Connection> b = connection.borrow()) {
            if (!b.isValid()) {
                return async.cancelled();
            }

            final Connection c = b.get();

            final String[] indices;

            try {
                indices = c.writeIndices(range);
            } catch (NoIndexSelectedException e) {
                return async.failed(e);
            }

            final String seriesId = Integer.toHexString(series.hashCode());

            final XContentBuilder xSeries;
            final BytesReference rawSeries;

            try {
                // convert to bytes, to avoid having to rebuild it for every write.
                // @formatter:off
                xSeries = XContentFactory.jsonBuilder();
                xSeries.startObject();
                Utils.buildMetadataDoc(xSeries, series);
                xSeries.endObject();

                // for nested entry in suggestion.
                final XContentBuilder xSeriesRaw = XContentFactory.jsonBuilder();
                xSeriesRaw.startObject();
                  xSeriesRaw.field("id", seriesId);
                  Utils.buildMetadataDoc(xSeriesRaw, series);
                xSeriesRaw.endObject();

                rawSeries = xSeriesRaw.bytes();
                // @formatter:on
            } catch (IOException e) {
                return async.failed(e);
            }

            final BulkProcessor bulk = c.bulk();

            final List<AsyncFuture<WriteResult>> futures = new ArrayList<>();

            for (final String index : indices) {
                final Pair<String, Series> key = Pair.of(index, series);

                final Callable<AsyncFuture<WriteResult>> loader =
                        new Callable<AsyncFuture<WriteResult>>() {
                            @Override
                            public AsyncFuture<WriteResult> call() throws Exception {
                                final Stopwatch watch = Stopwatch.createStarted();

                                bulk.add(new IndexRequest(index, Utils.TYPE_SERIES, seriesId)
                                        .source(xSeries).opType(OpType.CREATE));

                                for (final Map.Entry<String, String> e : series.getTags()
                                        .entrySet()) {
                                    final String suggestId =
                                            seriesId + ":" + Integer.toHexString(e.hashCode());
                                    final XContentBuilder suggest = XContentFactory.jsonBuilder();

                                    suggest.startObject();
                                    Utils.buildTagDoc(suggest, rawSeries, e);
                                    suggest.endObject();

                                    bulk.add(new IndexRequest(index, Utils.TYPE_TAG, suggestId)
                                            .source(suggest).opType(OpType.CREATE));
                                }

                                return async.resolved(
                                        WriteResult.of(watch.elapsed(TimeUnit.NANOSECONDS)));
                            }
                        };

                try {
                    futures.add(writeCache.get(key, loader));
                } catch (ExecutionException e) {
                    futures.add(async.failed(e));
                } catch (RateLimitExceededException e) {
                    reporter.reportWriteDroppedByRateLimit();
                }
            }

            return async.collect(futures, WriteResult.merger());
        }
    }

    private <S, T> AsyncFuture<T> bind(final ListenableActionFuture<S> actionFuture,
            final Transform<S, T> transform) {
        final ResolvableFuture<T> future = async.future();

        actionFuture.addListener(new ActionListener<S>() {
            @Override
            public void onResponse(S response) {
                final T result;

                try {
                    result = transform.transform(response);
                } catch (Exception e) {
                    future.fail(e);
                    return;
                }

                future.resolve(result);
            }

            @Override
            public void onFailure(Throwable e) {
                future.fail(e);
            }
        });

        return future;
    }

    private QueryBuilder match(String field, String value, MatchOptions options)
            throws IOException {
        final BoolQueryBuilder bool = QueryBuilders.boolQuery();

        // exact match
        bool.should(QueryBuilders.termQuery(field, value));

        final List<String> terms;

        try {
            terms = Utils.tokenize(analyzer, field, value);
        } catch (IOException e) {
            throw new IOException("failed to tokenize query", e);
        }

        for (final String term : terms) {
            // prefix on raw to match with non-term prefixes.
            bool.should(QueryBuilders.prefixQuery(String.format("%s.raw", field), term));
            // prefix on terms, to match on the prefix of any term.
            bool.should(QueryBuilders.prefixQuery(field, term));
            // prefix on exact term matches.
            bool.should(QueryBuilders.termQuery(field, term));
        }

        // optionall match fuzzy
        if (options.isFuzzy()) {
            bool.should(QueryBuilders.fuzzyQuery(field, value)
                    .prefixLength(options.getFuzzyPrefixLength())
                    .maxExpansions(options.getFuzzyMaxExpansions()));
        }

        return bool;
    }

    private static final class Utils {
        public static final String TYPE_TAG = "tag";
        public static final String TYPE_SERIES = "series";

        /**
         * Fields for type "series".
         **/
        public static final String SERIES_KEY = "key";
        public static final String SERIES_KEY_RAW = "key.raw";

        /**
         * Fields for type "metadata".
         */
        public static final String METADATA_KEY = "key";
        public static final String METADATA_TAGS = "tags";

        /**
         * Fields for type "tag".
         */
        public static final String TAG_KEY = "key";
        public static final String TAG_KEY_RAW = "key.raw";
        public static final String TAG_VALUE = "value";
        public static final String TAG_VALUE_RAW = "value.raw";
        public static final String TAG_KV = "kv";
        public static final String TAG_SERIES = "series";

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

        public static void buildTagDoc(final XContentBuilder b, BytesReference series,
                Entry<String, String> e) throws IOException {
            b.rawField(TAG_SERIES, series);
            b.field(TAG_KEY, e.getKey());
            b.field(TAG_VALUE, e.getValue());
            b.field(TAG_KV, e.getKey() + "\t" + e.getValue());
        }

        public static List<String> tokenize(Analyzer analyzer, String field, String keywords)
                throws IOException {
            final List<String> terms = new ArrayList<String>();

            try (final Reader reader = new StringReader(keywords)) {
                try (final TokenStream stream = analyzer.tokenStream(field, reader)) {
                    final CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);

                    stream.reset();

                    final String first = term.toString();

                    if (!first.isEmpty()) {
                        terms.add(first);
                    }

                    while (stream.incrementToken()) {
                        final String next = term.toString();

                        if (next.isEmpty()) {
                            continue;
                        }

                        terms.add(next);
                    }

                    stream.end();
                }
            }

            return terms;
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
                this(ImmutableList.<String> builder().add(path).build());
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

            public FilterBuilder filter(final Filter filter) {
                if (filter instanceof Filter.True) {
                    return matchAllFilter();
                }

                if (filter instanceof Filter.False) {
                    return null;
                }

                if (filter instanceof Filter.And) {
                    final Filter.And and = (Filter.And) filter;
                    final List<FilterBuilder> filters = new ArrayList<>(and.terms().size());

                    for (final Filter stmt : and.terms()) {
                        filters.add(filter(stmt));
                    }

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

                    final BoolFilterBuilder nested = boolFilter();
                    nested.must(termFilter(tagsKey, matchTag.first()));
                    nested.must(termFilter(tagsValue, matchTag.second()));
                    return nestedFilter(tags, nested);
                }

                if (filter instanceof Filter.StartsWith) {
                    final Filter.StartsWith startsWith = (Filter.StartsWith) filter;

                    final BoolFilterBuilder nested = boolFilter();
                    nested.must(termFilter(tagsKey, startsWith.first()));
                    nested.must(prefixFilter(tagsValue, startsWith.second()));
                    return nestedFilter(tags, nested);
                }

                if (filter instanceof Filter.Regex) {
                    final Filter.Regex regex = (Filter.Regex) filter;

                    final BoolFilterBuilder nested = boolFilter();
                    nested.must(termFilter(tagsKey, regex.first()));
                    nested.must(regexpFilter(tagsValue, regex.second()));
                    return nestedFilter(tags, nested);
                }

                if (filter instanceof Filter.HasTag) {
                    final Filter.HasTag hasTag = (Filter.HasTag) filter;
                    final TermFilterBuilder nested = termFilter(tagsKey, hasTag.first());
                    return nestedFilter(tags, nested);
                }

                if (filter instanceof Filter.MatchKey) {
                    final Filter.MatchKey matchKey = (Filter.MatchKey) filter;
                    return termFilter(seriesKey, matchKey.first());
                }

                throw new IllegalArgumentException("Invalid filter statement: " + filter);
            }
        }
    }

    public static BackendTypeFactory<SuggestBackend> factory() {
        return new BackendTypeFactory<SuggestBackend>() {
            @Override
            public BackendType<SuggestBackend> setup() {
                return new BackendType<SuggestBackend>() {
                    @Override
                    public Map<String, Map<String, Object>> mappings() throws IOException {
                        final Map<String, Map<String, Object>> mappings = new HashMap<>();
                        mappings.put("tag", loadJsonResource("v1/tag.json"));
                        mappings.put("series", loadJsonResource("v1/series.json"));
                        return mappings;
                    }

                    @Override
                    public Map<String, Object> settings() throws IOException {
                        return ImmutableMap.of();
                    }

                    @Override
                    public Class<? extends SuggestBackend> type() {
                        return SuggestBackendV1.class;
                    }
                };
            }
        };
    }
}
