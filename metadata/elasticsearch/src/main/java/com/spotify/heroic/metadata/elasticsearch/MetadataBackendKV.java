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

import static com.spotify.heroic.elasticsearch.ResourceLoader.loadJson;
import static java.util.Optional.ofNullable;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import com.google.common.hash.HashCode;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.elasticsearch.AbstractElasticsearchMetadataBackend;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.elasticsearch.SearchTransformResult;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.OrFilter;
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
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.StreamCollector;
import eu.toolchain.async.Transform;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

@ElasticsearchScope
public class MetadataBackendKV extends AbstractElasticsearchMetadataBackend
    implements MetadataBackend, LifeCycles {

    private static final Tracer tracer = Tracing.getTracer();
    private static final String WRITE_CACHE_SIZE = "write-cache-size";

    private static final String KEY = "key";
    private static final String TAGS = "tags";
    private static final String TAG_KEYS = "tag_keys";
    private static final String RESOURCE = "resource";
    private static final String HASH_FIELD = "hash";
    private static final Character TAG_DELIMITER = '\0';

    private static final String METADATA_TYPE = "metadata";

    private static final TimeValue SCROLL_TIME = TimeValue.timeValueMillis(5000);

    private final Groups groups;
    private final MetadataBackendReporter reporter;
    private final AsyncFramework async;
    private final Managed<Connection> connection;
    private final RateLimitedCache<Pair<String, HashCode>> writeCache;
    private final boolean configure;
    private final int deleteParallelism;
    private final int scrollSize;
    private final boolean indexResourceIdentifiers;

    @Inject
    public MetadataBackendKV(
        Groups groups,
        MetadataBackendReporter reporter,
        AsyncFramework async,
        Managed<Connection> connection,
        RateLimitedCache<Pair<String, HashCode>> writeCache,
        @Named("configure") boolean configure,
        @Named("deleteParallelism") int deleteParallelism,
        @Named("scrollSize") int scrollSize,
        @Named("indexResourceIdentifiers") boolean indexResourceIdentifiers
    ) {
        super(async, METADATA_TYPE, reporter);
        this.groups = groups;
        this.reporter = reporter;
        this.async = async;
        this.connection = connection;
        this.writeCache = writeCache;
        this.configure = configure;
        this.deleteParallelism = deleteParallelism;
        this.scrollSize = scrollSize;
        this.indexResourceIdentifiers = indexResourceIdentifiers;
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
        return write(request, tracer.getCurrentSpan());
    }

    @Override
    public AsyncFuture<WriteMetadata> write(
        final WriteMetadata.Request request, final Span parentSpan
    ) {
        return doto(c -> {
            final String rootSpanName = "MetadataBackendKV.write";
            final Span rootSpan = tracer
                .spanBuilderWithExplicitParent(rootSpanName, parentSpan)
                .startSpan();
            final Scope rootScope = tracer.withSpan(rootSpan);

            final Series series = request.getSeries();

            final String id = series.hash();
            rootSpan.putAttribute("id", AttributeValue.stringAttributeValue(id));

            final String[] indices;

            try {
                indices = c.getIndex().writeIndices(METADATA_TYPE);
            } catch (NoIndexSelectedException e) {
                rootSpan.setStatus(Status.INTERNAL.withDescription(e.toString()));
                rootSpan.end();
                return async.failed(e);
            }

            final List<AsyncFuture<WriteMetadata>> writes = new ArrayList<>();
            final List<Span> indexSpans = new ArrayList<>();

            for (final String index : indices) {
                final String indexSpanName = rootSpanName + ".index";
                final Span span = tracer.spanBuilder(indexSpanName).startSpan();
                try (Scope ignored = tracer.withSpan(span)) {
                    span.putAttribute("index", AttributeValue.stringAttributeValue(index));

                    if (!writeCache.acquire(Pair.of(index, series.getHashCodeTagOnly()),
                        reporter::reportWriteDroppedByCacheHit)) {
                        span.setStatus(
                            Status.ALREADY_EXISTS.withDescription("Write dropped by cache hit"));
                        span.end();
                        continue;
                    }
                    indexSpans.add(span);

                    final XContentBuilder source = XContentFactory.jsonBuilder();

                    source.startObject();
                    buildContext(source, series, indexResourceIdentifiers);
                    source.endObject();

                    IndexRequest indexRequest = new IndexRequest(index)
                        .id(id)
                        .source(source)
                        .opType(OpType.CREATE);

                    final RequestTimer<WriteMetadata> timer = WriteMetadata.timer();
                    final FutureReporter.Context writeContext =
                        reporter.setupBackendWriteReporter();

                    final Span writeSpan =
                        tracer.spanBuilder(indexSpanName + ".writeIndex").startSpan();

                    final ResolvableFuture<IndexResponse> result = async.future();
                    final ActionListener<IndexResponse> listener =
                        new ActionListener<>() {
                            @Override
                            public void onResponse(IndexResponse indexResponse) {
                                result.resolve(indexResponse);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                result.fail(e);
                            }
                        };

                    c.execute(indexRequest, listener);

                    final AsyncFuture<WriteMetadata> writeMetadataAsyncFuture = result
                        .directTransform(response -> timer.end())
                        .catchFailed(handleVersionConflict(WriteMetadata::new,
                            reporter::reportWriteDroppedByDuplicate))
                        .onDone(writeContext)
                        .onFinished(writeSpan::end);

                    writes.add(writeMetadataAsyncFuture);

                }
            }

            rootScope.close();
            return async.collect(writes, WriteMetadata.reduce()).onFinished(() -> {
                indexSpans.forEach(Span::end);
                rootSpan.end();
            });
        });
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final CountSeries.Request filter) {
        return doto(c -> {
            final OptionalLimit limit = filter.getLimit();

            if (limit.isZero()) {
                return async.resolved(new CountSeries());
            }

            final QueryBuilder f = filter(filter.getFilter());

            SearchRequest request = c.getIndex().count(METADATA_TYPE);
            SearchSourceBuilder sourceBuilder = request.source();
            limit.asInteger().ifPresent(sourceBuilder::terminateAfter);
            sourceBuilder.query(new BoolQueryBuilder().must(f));

            final ResolvableFuture<SearchResponse> future = async.future();
            c.execute(request, bind(future));

            return future.directTransform(
                r -> new CountSeries(r.getHits().getTotalHits().value, false));

        });
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final FindSeries.Request filter) {
        return entries(
            filter,
            this::toSeries,
            l -> new FindSeries(l.getSet(), l.isLimited()),
            request -> { });
    }

    @Override
    public AsyncObservable<FindSeriesStream> findSeriesStream(final FindSeries.Request request) {
        return entriesStream(
            request,
            this::toSeries,
            FindSeriesStream::new,
            builder -> { }
        );
    }

    @Override
    public AsyncFuture<FindSeriesIds> findSeriesIds(final FindSeries.Request request) {
        return entries(
            request,
            SearchHit::getId,
            l -> new FindSeriesIds(l.getSet(), l.isLimited()),
            searchRequest -> searchRequest.source().fetchSource(false)
        );
    }

    @Override
    public AsyncObservable<FindSeriesIdsStream> findSeriesIdsStream(
        final FindSeries.Request request
    ) {
        return entriesStream(
            request,
            SearchHit::getId,
            FindSeriesIdsStream::new,
            searchRequest -> searchRequest.source().fetchSource(false)
        );
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
        final DateRange range = request.getRange();

        final FindSeries.Request findIds = new FindSeries.Request(
            request.getFilter(), range, request.getLimit(), Features.DEFAULT);

        return doto(c -> findSeriesIds(findIds).lazyTransform(ids -> {
            final List<Callable<AsyncFuture<Void>>> deletes = new ArrayList<>();

            for (final String id : ids.getIds()) {
                deletes.add(() -> {
                    final List<DeleteRequest> requests = c.getIndex().delete(METADATA_TYPE, id);

                    return async.collectAndDiscard(requests
                        .stream()
                        .map(deleteRequest -> {
                            final ResolvableFuture<DeleteResponse> future = async.future();
                            c.execute(deleteRequest, bind(future));
                            return future;
                        })
                        .collect(Collectors.toList()));
                });
            }

            return async.eventuallyCollect(deletes, newDeleteCollector(), deleteParallelism);
        }));
    }


    @Override
    public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
        return doto(c -> {
            final QueryBuilder f = filter(request.getFilter());
            SearchRequest searchRequest = c.getIndex().search(METADATA_TYPE);
            searchRequest.source().query(new BoolQueryBuilder().must(f));

            {
                final AggregationBuilder terms = AggregationBuilders.terms("terms").field(KEY);
                searchRequest.source().aggregation(terms);
            }

            final ResolvableFuture<SearchResponse> future = async.future();
            future.resolve(c.execute(searchRequest));

            return future.directTransform(response -> {
                final Terms terms = response.getAggregations().get("terms");

                final Set<String> keys = new HashSet<>();

                int size = terms.getBuckets().size();
                int duplicates = 0;

                for (final Terms.Bucket bucket : terms.getBuckets()) {
                    if (keys.add(bucket.getKeyAsString())) {
                        duplicates += 1;
                    }
                }

                return new FindKeys(keys, size, duplicates);
            });
        });
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    protected Series toSeries(SearchHit hit) {
        final Map<String, Object> source = hit.getSourceAsMap();
        final String key = (String) source.get(KEY);

        @SuppressWarnings("unchecked")
        final Iterator<Map.Entry<String, String>> tags =
            ((List<String>) source.get(TAGS)).stream().map(this::buildTag).iterator();

        if (indexResourceIdentifiers) {
            @SuppressWarnings("unchecked")
            final Iterator<Map.Entry<String, String>> resource =
                ((List<String>) source.get(RESOURCE)).stream().map(this::buildTag).iterator();

            return Series.of(key, tags, resource);
        } else {
            return Series.of(key, tags);
        }
    }

    protected <T, O> AsyncFuture<O> entries(
        final FindSeries.Request seriesRequest,
        final Function<SearchHit, T> converter,
        final Transform<SearchTransformResult<T>, O> collector,
        final Consumer<SearchRequest> modifier
    ) {
        final QueryBuilder f = filter(seriesRequest.getFilter());
        OptionalLimit limit = seriesRequest.getLimit();

        return doto(c -> {
            SearchRequest request =
                c.getIndex().search(METADATA_TYPE).allowPartialSearchResults(false);

            request.source()
                .size(limit.asMaxInteger(scrollSize))
                .query(new BoolQueryBuilder().must(f))
                .sort(new FieldSortBuilder("hash"));

            modifier.accept(request);

            if (seriesRequest.getFeatures().hasFeature(Feature.METADATA_LIVE_CURSOR)) {
                return pageEntries(c, request, limit, converter)
                    .directTransform(collector);
            } else {
                request.scroll(SCROLL_TIME);
                return scrollEntries(c, request, limit, converter)
                    .onResolved(r ->
                        ofNullable(r.getLastScrollId()).ifPresent(c::clearSearchScroll))
                    .directTransform(collector);
            }
        });
    }

    /**
     * Collect the result of a list of operations and convert into a {@link
     * com.spotify.heroic.metadata.DeleteSeries}.
     *
     * @return a {@link eu.toolchain.async.StreamCollector}
     */
    private StreamCollector<Void, DeleteSeries> newDeleteCollector() {
        return new StreamCollector<>() {
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

            @Override
            public void resolved(final Void result) {
            }

            @Override
            public void failed(final Throwable cause) {
                errors.add(cause);
            }

            @Override
            public void cancelled() {
            }

            @Override
            public DeleteSeries end(
                final int resolved, final int failed, final int cancelled
            ) {
                final List<RequestError> errors = this.errors
                    .stream()
                    .map(QueryError::new)
                    .collect(Collectors.toList());

                return new DeleteSeries(errors, resolved, failed + cancelled);
            }
        };
    }

    private <T, O> AsyncObservable<O> entriesStream(
        final FindSeries.Request findRequest,
        final Function<SearchHit, T> converter,
        final Function<Set<T>, O> collector,
        final Consumer<SearchRequest> modifier
    ) {
        final QueryBuilder filter = filter(findRequest.getFilter());
        OptionalLimit limit = findRequest.getLimit();

        return observer -> connection.doto(c -> {
            SearchRequest request = c.getIndex().search(METADATA_TYPE).scroll(SCROLL_TIME);
            request.source()
                .size(limit.asMaxInteger(scrollSize))
                .query(new BoolQueryBuilder().must(filter));

            modifier.accept(request);

            final SearchTransformStream<T> scrollTransform =
                new SearchTransformStream<>(limit, set -> observer.observe(collector.apply(set)),
                    converter, scrollId -> {
                    // Function<> that returns a Supplier
                    return () -> {
                        final ResolvableFuture<SearchResponse> future = async.future();
                        c.searchScroll(scrollId, SCROLL_TIME, bind(future));
                        return future;
                    };
                });

            final ResolvableFuture<SearchResponse> future = async.future();
            c.execute(request, bind(future));

            return future.lazyTransform(scrollTransform);
        }).onDone(observer.onDone());
    }

    private <T> AsyncFuture<T> doto(ManagedAction<Connection, T> action) {
        return connection.doto(action);
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

    private Map.Entry<String, String> buildTag(String kv) {
        final int index = kv.indexOf(TAG_DELIMITER);

        if (index == -1) {
            throw new IllegalArgumentException("invalid tag source: " + kv);
        }

        final String tk = kv.substring(0, index);
        final String tv = kv.substring(index + 1);
        return Pair.of(tk, tv);
    }

    private static void buildContext(
        final XContentBuilder b,
        Series series,
        boolean indexResourceIdentifiers
    ) throws IOException {
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

        if (indexResourceIdentifiers) {
          b.startArray(RESOURCE);
          for (final Map.Entry<String, String> entry : series.getResource().entrySet()) {
            b.value(entry.getKey() + TAG_DELIMITER + entry.getValue());
          }
          b.endArray();
        }

        b.field(HASH_FIELD, series.hash());
    }

    private static final Filter.Visitor<QueryBuilder> FILTER_CONVERTER =
        new Filter.Visitor<>() {
            @Override
            public QueryBuilder visitTrue(final TrueFilter t) {
                return matchAllQuery();
            }

            @Override
            public QueryBuilder visitFalse(final FalseFilter f) {
                return new BoolQueryBuilder().mustNot(matchAllQuery());
            }

            @Override
            public QueryBuilder visitAnd(final AndFilter and) {
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                for (QueryBuilder qb : convertTerms(and.terms())) {
                    boolQuery.must(qb);
                }
                return boolQuery;
            }

            @Override
            public QueryBuilder visitOr(final OrFilter or) {
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                for (QueryBuilder qb : convertTerms(or.terms())) {
                    boolQuery.should(qb);
                }
                boolQuery.minimumShouldMatch(1);
                return boolQuery;
            }

            @Override
            public QueryBuilder visitNot(final NotFilter not) {
                return new BoolQueryBuilder().mustNot(not.filter().visit(this));
            }

            @Override
            public QueryBuilder visitMatchTag(final MatchTagFilter matchTag) {
                return termQuery(TAGS, matchTag.tag() + TAG_DELIMITER + matchTag.value());
            }

            @Override
            public QueryBuilder visitStartsWith(final StartsWithFilter startsWith) {
                return prefixQuery(TAGS,
                    startsWith.tag() + TAG_DELIMITER + startsWith.value());
            }

            @Override
            public QueryBuilder visitHasTag(final HasTagFilter hasTag) {
                return termQuery(TAG_KEYS, hasTag.tag());
            }

            @Override
            public QueryBuilder visitMatchKey(final MatchKeyFilter matchKey) {
                return termQuery(KEY, matchKey.key());
            }

            @Override
            public QueryBuilder defaultAction(final Filter filter) {
                throw new IllegalArgumentException("Unsupported filter statement: " + filter);
            }

            private QueryBuilder[] convertTerms(final List<Filter> terms) {
                final QueryBuilder[] filters = new QueryBuilder[terms.size()];
                int i = 0;

                for (final Filter stmt : terms) {
                    filters[i++] = stmt.visit(this);
                }

                return filters;
            }
        };

    @Override
    protected QueryBuilder filter(final Filter filter) {
        return filter.visit(FILTER_CONVERTER);
    }

    @Override
    public Statistics getStatistics() {
        return new Statistics(WRITE_CACHE_SIZE, writeCache.size());
    }

    static BackendType backendType() {
        final Map<String, Map<String, Object>> mappings = new HashMap<>();
        mappings.put(METADATA_TYPE, loadJson(MetadataBackendKV.class, "kv/metadata.json"));

        return new BackendType(MetadataBackendKV.class, mappings);
    }

    public String toString() {
        return "MetadataBackendKV(connection=" + this.connection + ")";
    }
}
