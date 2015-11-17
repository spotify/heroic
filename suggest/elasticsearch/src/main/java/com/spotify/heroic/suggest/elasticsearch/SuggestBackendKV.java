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
import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.prefixFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.io.IOException;
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

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;

import com.google.common.collect.ImmutableList;
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
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import lombok.ToString;

@ToString(of = { "connection" })
public class SuggestBackendKV implements SuggestBackend, LifeCycle, Grouped {
    private static final String TAG_DELIMITER = "\0";
    private static final String SERIES_TYPE = "series";
    private static final String TAG_TYPE = "tag";
    private static final String KEY = "key";
    private static final String TAG_KEYS = "tag_keys";
    private static final String TAGS = "tags";
    private static final String SERIES_ID = "series_id";

    private static final String TAG_SKEY_RAW = "skey.raw";
    private static final String TAG_SKEY_PREFIX = "skey.prefix";
    private static final String TAG_SKEY = "skey";
    private static final String TAG_SVAL_RAW = "sval.raw";
    private static final String TAG_SVAL_PREFIX = "sval.prefix";
    private static final String TAG_SVAL = "sval";
    private static final String TAG_KV = "kv";

    private static final String SERIES_KEY_ANALYZED = "key.analyzed";
    private static final String SERIES_KEY_PREFIX = "key.prefix";
    private static final String SERIES_KEY = "key";

    public static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private static final String[] KEY_SUGGEST_SOURCES = new String[] {KEY};
    private static final String[] TAG_SUGGEST_SOURCES = new String[] {TAG_SKEY, TAG_SVAL};

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
    public SuggestBackendKV(final AsyncFramework async, final Managed<Connection> connection,
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
                final BoolFilterBuilder bool = boolFilter();

                if (!(filter.getFilter() instanceof Filter.True)) {
                    bool.must(filter(filter.getFilter()));
                }

                for (final String e : exclude) {
                    bool.mustNot(termFilter(TAG_SKEY_RAW, e));
                }

                final QueryBuilder query = bool.hasClauses() ? filteredQuery(matchAllQuery(), bool)
                        : matchAllQuery();

                final SearchRequestBuilder request = c.search(filter.getRange(), TAG_TYPE)
                        .setSearchType(SearchType.COUNT).setQuery(query).setTimeout(TIMEOUT);

                {
                    final TermsBuilder terms = AggregationBuilders.terms("keys").field(TAG_SKEY_RAW)
                            .size(filter.getLimit() + 1);
                    request.addAggregation(terms);
                    // make value bucket one entry larger than necessary to figure out when limiting
                    // is applied.
                    final TermsBuilder cardinality = AggregationBuilders.terms("values")
                            .field(TAG_SVAL_RAW).size(groupLimit + 1);
                    terms.subAggregation(cardinality);
                }

                return bind(request.execute(), new Transform<SearchResponse, TagValuesSuggest>() {
                    @Override
                    public TagValuesSuggest transform(SearchResponse response) {
                        final List<TagValuesSuggest.Suggestion> suggestions = new ArrayList<>();

                        final Terms terms = (Terms) response.getAggregations().get("keys");

                        final List<Bucket> buckets = terms.getBuckets();

                        for (final Terms.Bucket bucket : buckets.subList(0,
                                Math.min(buckets.size(), filter.getLimit()))) {
                            final Terms valueTerms = bucket.getAggregations().get("values");

                            final List<Bucket> valueBuckets = valueTerms.getBuckets();

                            final SortedSet<String> result = new TreeSet<>();

                            for (final Terms.Bucket valueBucket : valueBuckets) {
                                result.add(valueBucket.getKey());
                            }

                            final boolean limited = valueBuckets.size() > groupLimit;

                            final ImmutableList<String> values = ImmutableList.copyOf(result)
                                    .subList(0, Math.min(result.size(), groupLimit));

                            suggestions.add(new TagValuesSuggest.Suggestion(bucket.getKey(), values,
                                    limited));
                        }

                        return new TagValuesSuggest(new ArrayList<>(suggestions),
                                buckets.size() > filter.getLimit());
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
                final BoolQueryBuilder bool = boolQuery();

                if (key != null && !key.isEmpty()) {
                    bool.must(termQuery(TAG_SKEY_RAW, key));
                }

                QueryBuilder query = bool.hasClauses() ? bool : matchAllQuery();

                if (!(filter.getFilter() instanceof Filter.True)) {
                    query = filteredQuery(query, filter(filter.getFilter()));
                }

                final SearchRequestBuilder request = c.search(filter.getRange(), TAG_TYPE)
                        .setSearchType(SearchType.COUNT).setQuery(query);

                {
                    final TermsBuilder terms = AggregationBuilders.terms("values")
                            .field(TAG_SVAL_RAW).size(filter.getLimit() + 1)
                            .order(Order.term(true));
                    request.addAggregation(terms);
                }

                return bind(request.execute(), new Transform<SearchResponse, TagValueSuggest>() {
                    @Override
                    public TagValueSuggest transform(SearchResponse response) {
                        final List<String> suggestions = new ArrayList<>();

                        final Terms terms = (Terms) response.getAggregations().get("values");

                        final List<Bucket> buckets = terms.getBuckets();

                        for (final Terms.Bucket bucket : buckets.subList(0,
                                Math.min(buckets.size(), filter.getLimit()))) {
                            suggestions.add(bucket.getKey());
                        }

                        boolean limited = buckets.size() > filter.getLimit();

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
                final QueryBuilder root = filteredQuery(matchAllQuery(),
                        filter(filter.getFilter()));

                final SearchRequestBuilder request = c.search(filter.getRange(), TAG_TYPE)
                        .setSearchType(SearchType.COUNT).setQuery(root);

                final int limit = filter.getLimit();

                {
                    final TermsBuilder terms = AggregationBuilders.terms("keys").field(TAG_SKEY_RAW)
                            .size(limit + 1);
                    request.addAggregation(terms);
                }

                return bind(request.execute(), new Transform<SearchResponse, TagKeyCount>() {
                    @Override
                    public TagKeyCount transform(SearchResponse response) {
                        final Set<TagKeyCount.Suggestion> suggestions = new LinkedHashSet<>();

                        final Terms terms = (Terms) response.getAggregations().get("keys");

                        final List<Bucket> buckets = terms.getBuckets();

                        for (final Terms.Bucket bucket : buckets.subList(0,
                                Math.min(buckets.size(), limit))) {
                            suggestions.add(new TagKeyCount.Suggestion(bucket.getKey(),
                                    bucket.getDocCount()));
                        }

                        return new TagKeyCount(new ArrayList<>(suggestions),
                                terms.getBuckets().size() < limit);
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
                final BoolQueryBuilder bool = boolQuery();

                if (key != null && !key.isEmpty()) {
                    final String l = key.toLowerCase();
                    final BoolQueryBuilder sub = boolQuery();
                    sub.should(termQuery(TAG_SKEY, l));
                    sub.should(termQuery(TAG_SKEY_PREFIX, l).boost(1.5f));
                    sub.should(termQuery(TAG_SKEY_RAW, l).boost(2.0f));
                    bool.must(sub);
                }

                if (value != null && !value.isEmpty()) {
                    final String l = value.toLowerCase();
                    final BoolQueryBuilder sub = boolQuery();
                    sub.should(termQuery(TAG_SVAL, l));
                    sub.should(termQuery(TAG_SVAL_PREFIX, l).boost(1.5f));
                    sub.should(termQuery(TAG_SVAL_RAW, l).boost(2.0f));
                    bool.must(sub);
                }

                QueryBuilder query = bool.hasClauses() ? bool : matchAllQuery();

                if (!(filter.getFilter() instanceof Filter.True)) {
                    query = filteredQuery(query, filter(filter.getFilter()));
                }

                final SearchRequestBuilder request = c.search(filter.getRange(), TAG_TYPE)
                        .setSearchType(SearchType.COUNT).setQuery(query).setTimeout(TIMEOUT);

                // aggregation
                {
                    final TopHitsBuilder hits = AggregationBuilders.topHits("hits").setSize(1)
                            .setFetchSource(TAG_SUGGEST_SOURCES, new String[0]);

                    final TermsBuilder terms = AggregationBuilders.terms("terms").field(TAG_KV)
                            .size(filter.getLimit()).subAggregation(hits);

                    request.addAggregation(terms);
                }

                return bind(request.execute(), new Transform<SearchResponse, TagSuggest>() {
                    @Override
                    public TagSuggest transform(SearchResponse response) throws Exception {
                        final Set<Suggestion> suggestions = new LinkedHashSet<>();

                        Aggregations aggregations = response.getAggregations();

                        if (aggregations == null) {
                            return TagSuggest.empty();
                        }

                        final StringTerms terms = (StringTerms) aggregations.get("terms");

                        for (final Terms.Bucket bucket : terms.getBuckets()) {
                            final TopHits topHits = (TopHits) bucket.getAggregations().get("hits");
                            final SearchHits hits = topHits.getHits();
                            final SearchHit hit = hits.getAt(0);
                            final Map<String, Object> doc = hit.getSource();

                            final String key = (String) doc.get(TAG_SKEY);
                            final String value = (String) doc.get(TAG_SVAL);

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
                BoolQueryBuilder bool = boolQuery();

                if (key != null && !key.isEmpty()) {
                    final String l = key.toLowerCase();
                    final BoolQueryBuilder b = boolQuery();
                    b.should(termQuery(SERIES_KEY_ANALYZED, l));
                    b.should(termQuery(SERIES_KEY_PREFIX, l).boost(1.5f));
                    b.should(termQuery(SERIES_KEY, l).boost(2.0f));
                    bool.must(b);
                }

                QueryBuilder query = bool.hasClauses() ? bool : matchAllQuery();

                if (!(filter.getFilter() instanceof Filter.True)) {
                    query = filteredQuery(query, filter(filter.getFilter()));
                }

                final SearchRequestBuilder request = c.search(filter.getRange(), "series")
                        .setSearchType(SearchType.COUNT).setQuery(query);

                // aggregation
                {
                    final TopHitsBuilder hits = AggregationBuilders.topHits("hits").setSize(1)
                            .setFetchSource(KEY_SUGGEST_SOURCES, new String[0]);

                    final TermsBuilder terms = AggregationBuilders.terms("terms").field(KEY)
                            .size(filter.getLimit()).subAggregation(hits);

                    request.addAggregation(terms);
                }

                return bind(request.execute(), new Transform<SearchResponse, KeySuggest>() {
                    @Override
                    public KeySuggest transform(SearchResponse response) throws Exception {
                        final Set<KeySuggest.Suggestion> suggestions = new LinkedHashSet<>();

                        final StringTerms terms = (StringTerms) response.getAggregations()
                                .get("terms");

                        for (final Terms.Bucket bucket : terms.getBuckets()) {
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
    public AsyncFuture<WriteResult> write(final Series s, final DateRange range) {
        return doto(new ManagedAction<Connection, WriteResult>() {
            @Override
            public AsyncFuture<WriteResult> action(final Connection c) throws Exception {
                final String[] indices;

                try {
                    indices = c.writeIndices(range);
                } catch (NoIndexSelectedException e) {
                    return async.failed(e);
                }

                final List<AsyncFuture<WriteResult>> writes = new ArrayList<>();

                for (final String index : indices) {
                    final Pair<String, Series> key = Pair.of(index, s);

                    final Callable<AsyncFuture<WriteResult>> loader = () -> {
                            final String seriesId = s.hash();

                            final List<AsyncFuture<WriteResult>> w = new ArrayList<>();

                            final XContentBuilder series = XContentFactory.jsonBuilder();

                            series.startObject();
                            buildContext(series, s);
                            series.endObject();

                            final long start = System.nanoTime();

                            w.add(bind(
                                    c.index(index, SERIES_TYPE).setId(seriesId).setSource(series)
                                            .setOpType(OpType.CREATE).execute(),
                                    (response) -> WriteResult.of(System.nanoTime() - start)));

                            for (final Map.Entry<String, String> e : s.getTags().entrySet()) {
                                final XContentBuilder suggest = XContentFactory.jsonBuilder();

                                suggest.startObject();
                                buildContext(suggest, s);
                                buildTag(suggest, e);
                                suggest.endObject();

                                final String suggestId = seriesId + ":"
                                        + Integer.toHexString(e.hashCode());

                                w.add(bind(
                                        c.index(index, TAG_TYPE).setId(suggestId).setSource(suggest)
                                                .setOpType(OpType.CREATE).execute(),
                                        (response) -> WriteResult.of(System.nanoTime() - start)));
                            }

                            return async.collect(w, WriteResult.merger());
                    };

                    try {
                        writes.add(writeCache.get(key, loader));
                    } catch (ExecutionException e) {
                        return async.failed(e);
                    } catch (RateLimitExceededException e) {
                        reporter.reportWriteDroppedByRateLimit();
                    }
                }

                return async.collect(writes, WriteResult.merger());
            }
        });
    }

    /**
     * Bind a {@code ListenableActionFuture} to an {@code AsyncFuture}.
     *
     * @param actionFuture
     * @param transform
     * @return
     */
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

    static void buildContext(final XContentBuilder b, final Series series) throws IOException {
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

        b.field(SERIES_ID, series.hash());
    }

    static void buildTag(final XContentBuilder b, Entry<String, String> e) throws IOException {
        b.field(TAG_SKEY, e.getKey());
        b.field(TAG_SVAL, e.getValue());
        b.field(TAG_KV, e.getKey() + TAG_DELIMITER + e.getValue());
    }

    public static FilterBuilder filter(final Filter filter) {
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

    public static BackendTypeFactory<SuggestBackend> factory() {
        return new BackendTypeFactory<SuggestBackend>() {
            @Override
            public BackendType<SuggestBackend> setup() {
                return new BackendType<SuggestBackend>() {
                    @Override
                    public Map<String, Map<String, Object>> mappings() throws IOException {
                        final Map<String, Map<String, Object>> mappings = new HashMap<>();
                        mappings.put("tag", loadJsonResource("kv/tag.json"));
                        mappings.put("series", loadJsonResource("kv/series.json"));
                        return mappings;
                    }

                    @Override
                    public Map<String, Object> settings() throws IOException {
                        return loadJsonResource("kv/settings.json");
                    }

                    @Override
                    public Class<? extends SuggestBackend> type() {
                        return SuggestBackendKV.class;
                    }
                };
            }
        };
    }
}
