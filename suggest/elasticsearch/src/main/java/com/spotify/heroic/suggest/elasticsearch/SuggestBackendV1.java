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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.AbstractElasticsearchBackend;
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
import com.spotify.heroic.statistics.SuggestBackendReporter;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagSuggest.Suggestion;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import com.spotify.heroic.suggest.WriteSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesReference;
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

import javax.inject.Inject;
import javax.inject.Named;
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

@ElasticsearchScope
@ToString(of = {"connection"})
public class SuggestBackendV1 extends AbstractElasticsearchBackend
    implements SuggestBackend, Grouped, LifeCycles {
    private static final StandardAnalyzer analyzer = new StandardAnalyzer();

    // different locations for the series used in filtering.
    private static final Utils.FilterContext SERIES_CTX = Utils.context();
    private static final Utils.FilterContext TAG_CTX = Utils.context(Utils.TAG_SERIES);

    private static final String[] KEY_SUGGEST_SOURCES = new String[]{Utils.SERIES_KEY_RAW};

    private static final String[] TAG_SUGGEST_SOURCES =
        new String[]{Utils.TAG_KEY, Utils.TAG_VALUE};

    private final Managed<Connection> connection;
    private final SuggestBackendReporter reporter;
    /**
     * prevent unnecessary writes if entry is already in cache. Integer is the hashCode of the
     * series.
     */
    private final RateLimitedCache<Pair<String, HashCode>> writeCache;
    private final Groups groups;
    private final boolean configure;

    @Inject
    public SuggestBackendV1(
        final AsyncFramework async, final Managed<Connection> connection,
        final SuggestBackendReporter reporter,
        final RateLimitedCache<Pair<String, HashCode>> writeCache, final Groups groups,
        @Named("configure") boolean configure
    ) {
        super(async);
        this.connection = connection;
        this.reporter = reporter;
        this.writeCache = writeCache;
        this.groups = groups;
        this.configure = configure;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    @Override
    public AsyncFuture<Void> configure() {
        return connection.doto(Connection::configure);
    }

    @Override
    public Groups groups() {
        return groups;
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(final TagValuesSuggest.Request request) {
        return connection.doto((final Connection c) -> {
            final FilterBuilder f = TAG_CTX.filter(request.getFilter());

            final BoolQueryBuilder root = QueryBuilders.boolQuery();
            root.must(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            for (final String e : request.getExclude()) {
                root.mustNot(QueryBuilders.matchQuery(Utils.TAG_KEY_RAW, e));
            }

            final SearchRequestBuilder builder;

            try {
                builder = c
                    .search(request.getRange(), Utils.TYPE_TAG)
                    .setSearchType(SearchType.COUNT)
                    .setQuery(root);
            } catch (NoIndexSelectedException e) {
                return async.failed(e);
            }

            final OptionalLimit limit = request.getLimit();
            final OptionalLimit groupLimit = request.getGroupLimit();

            {
                final TermsBuilder terms =
                    AggregationBuilders.terms("keys").field(Utils.TAG_KEY_RAW);

                limit.asInteger().ifPresent(l -> terms.size(l + 1));

                builder.addAggregation(terms);
                // make value bucket one entry larger than necessary to figure out when limiting
                // is applied.
                final TermsBuilder cardinality =
                    AggregationBuilders.terms("values").field(Utils.TAG_VALUE_RAW);

                groupLimit.asInteger().ifPresent(l -> cardinality.size(l + 1));
                terms.subAggregation(cardinality);
            }

            return bind(builder.execute()).directTransform((SearchResponse response) -> {
                final List<TagValuesSuggest.Suggestion> suggestions = new ArrayList<>();

                final Terms terms = response.getAggregations().get("keys");

                final List<Bucket> suggestionBuckets = terms.getBuckets();

                for (final Terms.Bucket bucket : limit.limitList(suggestionBuckets)) {
                    final Terms valueTerms = bucket.getAggregations().get("values");

                    final List<Bucket> valueBuckets = valueTerms.getBuckets();

                    final SortedSet<String> result = new TreeSet<>();

                    for (final Terms.Bucket valueBucket : valueBuckets) {
                        result.add(valueBucket.getKey());
                    }

                    final boolean limited = groupLimit.isGreater(valueBuckets.size());
                    final SortedSet<String> values = groupLimit.limitSortedSet(result);

                    suggestions.add(
                        new TagValuesSuggest.Suggestion(bucket.getKey(), values, limited));
                }

                return TagValuesSuggest.of(ImmutableList.copyOf(suggestions),
                    limit.isGreater(suggestionBuckets.size()));
            });
        });
    }

    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
        return connection.doto((final Connection c) -> {
            final BoolQueryBuilder root = QueryBuilders.boolQuery();

            request.getKey().ifPresent(k -> {
                if (!k.isEmpty()) {
                    root.must(QueryBuilders.termQuery(Utils.TAG_KEY_RAW, k));
                }
            });

            root.must(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),
                TAG_CTX.filter(request.getFilter())));

            final SearchRequestBuilder builder = c
                .search(request.getRange(), Utils.TYPE_TAG)
                .setSearchType(SearchType.COUNT)
                .setQuery(root);

            final OptionalLimit limit = request.getLimit();

            {
                final TermsBuilder terms = AggregationBuilders
                    .terms("values")
                    .field(Utils.TAG_VALUE_RAW)
                    .order(Order.term(true));

                limit.asInteger().ifPresent(l -> terms.size(l + 1));
                builder.addAggregation(terms);
            }

            return bind(builder.execute()).directTransform((SearchResponse response) -> {
                final ImmutableList.Builder<String> suggestions = ImmutableList.builder();

                final Terms terms = response.getAggregations().get("values");

                final List<Bucket> all = terms.getBuckets();

                for (final Terms.Bucket bucket : limit.limitList(all)) {
                    suggestions.add(bucket.getKey());
                }

                return TagValueSuggest.of(suggestions.build(), limit.isGreater(all.size()));
            });
        });
    }

    @Override
    public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
        return connection.doto((final Connection c) -> {
            final FilterBuilder f = TAG_CTX.filter(request.getFilter());

            final BoolQueryBuilder root = QueryBuilders.boolQuery();
            root.must(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f));

            final SearchRequestBuilder builder;

            try {
                builder = c
                    .search(request.getRange(), Utils.TYPE_TAG)
                    .setSearchType(SearchType.COUNT)
                    .setQuery(root);
            } catch (NoIndexSelectedException e) {
                return async.failed(e);
            }

            final OptionalLimit limit = request.getLimit();

            {
                final TermsBuilder terms =
                    AggregationBuilders.terms("keys").field(Utils.TAG_KEY_RAW);

                limit.asInteger().ifPresent(terms::size);

                builder.addAggregation(terms);
                final CardinalityBuilder cardinality =
                    AggregationBuilders.cardinality("cardinality").field(Utils.TAG_VALUE_RAW);
                terms.subAggregation(cardinality);
            }

            return bind(builder.execute()).directTransform((SearchResponse response) -> {
                final Set<TagKeyCount.Suggestion> suggestions = new LinkedHashSet<>();

                final Terms terms = response.getAggregations().get("keys");

                for (final Terms.Bucket bucket : terms.getBuckets()) {
                    final Cardinality cardinality = bucket.getAggregations().get("cardinality");
                    suggestions.add(
                        new TagKeyCount.Suggestion(bucket.getKey(), cardinality.getValue()));
                }

                return TagKeyCount.of(ImmutableList.copyOf(suggestions), false);
            });
        });
    }

    @Override
    public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
        return connection.doto((Connection c) -> {
            final QueryBuilder query;

            final BoolQueryBuilder fuzzy = QueryBuilders.boolQuery();

            request.getKey().ifPresent(k -> {
                if (!k.isEmpty()) {
                    fuzzy.should(match(Utils.TAG_KEY, k, request.getOptions()));
                }
            });

            request.getValue().ifPresent(v -> {
                if (!v.isEmpty()) {
                    fuzzy.should(match(Utils.TAG_VALUE, v, request.getOptions()));
                }
            });

            if (request.getFilter() instanceof TrueFilter) {
                query = fuzzy;
            } else {
                query = QueryBuilders.filteredQuery(fuzzy, TAG_CTX.filter(request.getFilter()));
            }

            final SearchRequestBuilder builder = c
                .search(request.getRange(), Utils.TYPE_TAG)
                .setSearchType(SearchType.COUNT)
                .setQuery(query);

            // aggregation
            {
                final MaxBuilder topHit = AggregationBuilders.max("topHit").script("_score");
                final TopHitsBuilder hits = AggregationBuilders
                    .topHits("hits")
                    .setSize(1)
                    .setFetchSource(TAG_SUGGEST_SOURCES, new String[0]);

                final TermsBuilder kvs = AggregationBuilders
                    .terms("kvs")
                    .field(Utils.TAG_KV)
                    .order(Order.aggregation("topHit", false))
                    .subAggregation(hits)
                    .subAggregation(topHit);

                request.getLimit().asInteger().ifPresent(kvs::size);
                builder.addAggregation(kvs);
            }

            return bind(builder.execute()).directTransform((SearchResponse response) -> {
                final ImmutableList.Builder<Suggestion> suggestions = ImmutableList.builder();

                final StringTerms kvs = response.getAggregations().get("kvs");

                for (final Terms.Bucket bucket : kvs.getBuckets()) {
                    final TopHits topHits = bucket.getAggregations().get("hits");
                    final SearchHits hits = topHits.getHits();
                    final SearchHit hit = hits.getAt(0);
                    final Map<String, Object> doc = hit.getSource();

                    final String k = (String) doc.get(Utils.TAG_KEY);
                    final String v = (String) doc.get(Utils.TAG_VALUE);
                    suggestions.add(new Suggestion(hits.getMaxScore(), k, v));
                }

                return TagSuggest.of(suggestions.build());
            });
        });
    }

    @Override
    public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
        return connection.doto((final Connection c) -> {
            final QueryBuilder query;

            final BoolQueryBuilder fuzzy = QueryBuilders.boolQuery();

            request.getKey().ifPresent(k -> {
                if (!k.isEmpty()) {
                    fuzzy.should(match(Utils.SERIES_KEY, k, request.getOptions()));
                }
            });

            if (request.getFilter() instanceof TrueFilter) {
                query = fuzzy;
            } else {
                query = QueryBuilders.filteredQuery(fuzzy, SERIES_CTX.filter(request.getFilter()));
            }

            final SearchRequestBuilder builder = c
                .search(request.getRange(), Utils.TYPE_SERIES)
                .setSearchType(SearchType.COUNT)
                .setQuery(query);

            // aggregation
            {
                final MaxBuilder topHit = AggregationBuilders.max("top_hit").script("_score");
                final TopHitsBuilder hits = AggregationBuilders
                    .topHits("hits")
                    .setSize(1)
                    .setFetchSource(KEY_SUGGEST_SOURCES, new String[0]);

                final TermsBuilder keys = AggregationBuilders
                    .terms("keys")
                    .field(Utils.SERIES_KEY_RAW)
                    .order(Order.aggregation("top_hit", false))
                    .subAggregation(hits)
                    .subAggregation(topHit);

                request.getLimit().asInteger().ifPresent(keys::size);
                builder.addAggregation(keys);
            }

            return bind(builder.execute()).directTransform((SearchResponse response) -> {
                final Set<KeySuggest.Suggestion> suggestions = new LinkedHashSet<>();

                final StringTerms keys = response.getAggregations().get("keys");

                for (final Terms.Bucket bucket : keys.getBuckets()) {
                    final TopHits topHits = bucket.getAggregations().get("hits");
                    final SearchHits hits = topHits.getHits();
                    suggestions.add(new KeySuggest.Suggestion(hits.getMaxScore(), bucket.getKey()));
                }

                return KeySuggest.of(ImmutableList.copyOf(suggestions));
            });
        });
    }

    @Override
    public AsyncFuture<WriteSuggest> write(final WriteSuggest.Request request) {
        return connection.doto((final Connection c) -> {
            final Series series = request.getSeries();
            final DateRange range = request.getRange();

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

            final List<AsyncFuture<WriteSuggest>> futures = new ArrayList<>();

            for (final String index : indices) {
                final Pair<String, HashCode> key = Pair.of(index, series.getHashCode());

                if (!writeCache.acquire(key)) {
                    reporter.reportWriteDroppedByRateLimit();
                    continue;
                }

                final RequestTimer<WriteSuggest> timer = WriteSuggest.timer();

                futures.add(bind(c
                    .index(index, Utils.TYPE_SERIES)
                    .setId(seriesId)
                    .setSource(xSeries)
                    .setOpType(OpType.CREATE)
                    .execute()).directTransform(result -> timer.end()));

                try {
                    for (final Map.Entry<String, String> e : series.getTags().entrySet()) {
                        final String suggestId = seriesId + ":" + Integer.toHexString(e.hashCode());
                        final XContentBuilder suggest = XContentFactory.jsonBuilder();

                        suggest.startObject();
                        Utils.buildTagDoc(suggest, rawSeries, e);
                        suggest.endObject();

                        futures.add(bind(c
                            .index(index, Utils.TYPE_TAG)
                            .setId(suggestId)
                            .setSource(suggest)
                            .setOpType(OpType.CREATE)
                            .execute()).directTransform(result -> timer.end()));
                    }
                } catch (final Exception e) {
                    return async.failed(e);
                }
            }

            return async.collect(futures, WriteSuggest.reduce());
        });
    }

    private AsyncFuture<Void> start() {
        final AsyncFuture<Void> future = connection.start();

        if (!configure) {
            return future;
        }

        return future.lazyTransform(v -> configure());
    }

    private AsyncFuture<Void> stop() {
        return connection.stop();
    }

    private QueryBuilder match(String field, String value, MatchOptions options) {
        final BoolQueryBuilder bool = QueryBuilders.boolQuery();

        // exact match
        bool.should(QueryBuilders.termQuery(field, value));

        final List<String> terms;

        try {
            terms = Utils.tokenize(analyzer, field, value);
        } catch (final IOException e) {
            throw new RuntimeException("failed to tokenize query", e);
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
            bool.should(QueryBuilders
                .fuzzyQuery(field, value)
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

        public static void buildTagDoc(
            final XContentBuilder b, BytesReference series, Entry<String, String> e
        ) throws IOException {
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

                        return orFilter(filters.toArray(new FilterBuilder[0]));
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

    public static BackendType backendType() {
        final Map<String, Map<String, Object>> mappings = new HashMap<>();
        mappings.put("tag", loadJsonResource("v1/tag.json"));
        mappings.put("series", loadJsonResource("v1/series.json"));
        return new BackendType(mappings, ImmutableMap.of(), SuggestBackendV1.class);
    }
}
