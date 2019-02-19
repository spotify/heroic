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

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.Entries;
import com.spotify.heroic.metadata.MetadataBackend;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Managed;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Data;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

public abstract class AbstractElasticsearchMetadataBackend extends AbstractElasticsearchBackend
    implements MetadataBackend {
    public static final TimeValue SCROLL_TIME = TimeValue.timeValueSeconds(5);

    private final String type;

    public AbstractElasticsearchMetadataBackend(final AsyncFramework async, final String type) {
        super(async);
        this.type = type;
    }

    protected abstract Managed<Connection> connection();

    protected abstract QueryBuilder filter(Filter filter);

    protected abstract Series toSeries(SearchHit hit);

    protected <T> AsyncFuture<LimitedSet<T>> scrollEntries(
        final Connection c, final SearchRequestBuilder request, final OptionalLimit limit,
        final Function<SearchHit, T> converter
    ) {
        final ScrollTransform<T> scrollTransform =
            new ScrollTransform<T>(async, limit, converter, scrollId -> {
                // Function<> that returns a Supplier
                return () -> {
                    final ListenableActionFuture<SearchResponse> execute =
                        c.prepareSearchScroll(scrollId).setScroll(SCROLL_TIME).execute();
                    return bind(execute);
                };
            });

        return bind(request.execute()).lazyTransform(scrollTransform);
    }

    public static class ScrollTransform<T> implements LazyTransform<SearchResponse, LimitedSet<T>> {
        private final AsyncFramework async;
        private final OptionalLimit limit;

        int size = 0;
        int duplicates = 0;
        final Set<T> results = new HashSet<>();
        final Function<SearchHit, T> converter;
        final Function<String, Supplier<AsyncFuture<SearchResponse>>> scrollFactory;

        @java.beans.ConstructorProperties({ "async", "limit", "converter", "scrollFactory" })
        public ScrollTransform(
            final AsyncFramework async,
            final OptionalLimit limit,
            final Function<SearchHit, T> converter,
            final Function<String, Supplier<AsyncFuture<SearchResponse>>> scrollFactory
        ) {
            this.async = async;
            this.limit = limit;
            this.converter = converter;
            this.scrollFactory = scrollFactory;
        }

        @Override
        public AsyncFuture<LimitedSet<T>> transform(final SearchResponse response)
            throws Exception {
            final SearchHit[] hits = response.getHits().getHits();

            for (final SearchHit hit : hits) {
                final T convertedHit = converter.apply(hit);

                if (!results.add(convertedHit)) {
                    duplicates += 1;
                } else {
                    size += 1;
                }

                if (limit.isGreater(size)) {
                    results.remove(convertedHit);
                    return async.resolved(new LimitedSet<>(limit.limitSet(results), true));
                }
            }

            if (hits.length == 0) {
                return async.resolved(new LimitedSet<>(results, false));
            }

            final String scrollId = response.getScrollId();
            if (scrollId == null) {
                return async.resolved(LimitedSet.of());
            }

            // Fetch the next page in the scrolling search. The result will be handled by a new call
            // to this method.
            final Supplier<AsyncFuture<SearchResponse>> scroller = scrollFactory.apply(scrollId);
            return scroller.get().lazyTransform(this);
        }
    }

    public static class ScrollTransformStream<T> implements LazyTransform<SearchResponse, Void> {
        private final OptionalLimit limit;
        private final Function<Set<T>, AsyncFuture<Void>> seriesFunction;
        private final Function<SearchHit, T> converter;
        private final Function<String, Supplier<AsyncFuture<SearchResponse>>> scrollFactory;

        int size = 0;

        @java.beans.ConstructorProperties({ "limit", "seriesFunction", "converter",
                                            "scrollFactory" })
        public ScrollTransformStream(
            final OptionalLimit limit,
            final Function<Set<T>, AsyncFuture<Void>> seriesFunction,
            final Function<SearchHit, T> converter,
            final Function<String, Supplier<AsyncFuture<SearchResponse>>> scrollFactory
        ) {
            this.limit = limit;
            this.seriesFunction = seriesFunction;
            this.converter = converter;
            this.scrollFactory = scrollFactory;
        }

        @Override
        public AsyncFuture<Void> transform(final SearchResponse response) throws Exception {
            final SearchHit[] hits = response.getHits().getHits();

            final Set<T> batch = new HashSet<>();

            for (final SearchHit hit : hits) {
                if (limit.isGreaterOrEqual(size)) {
                    break;
                }

                batch.add(converter.apply(hit));
                size += 1;
            }

            if (hits.length == 0 || limit.isGreaterOrEqual(size)) {
                return seriesFunction.apply(batch);
            }

            final String scrollId = response.getScrollId();
            if (scrollId == null) {
                return seriesFunction.apply(batch);
            }

            // Fetch the next page in the scrolling search. The result will be handled by a new call
            // to this method.
            return seriesFunction.apply(batch).lazyTransform(v -> {
                return scrollFactory.apply(scrollId).get().lazyTransform(this);
            });
        }
    }

    private static final int ENTRIES_SCAN_SIZE = 1000;
    private static final TimeValue ENTRIES_TIMEOUT = TimeValue.timeValueSeconds(5);

    @Override
    public AsyncObservable<Entries> entries(final Entries.Request request) {
        final QueryBuilder f = filter(request.getFilter());

        final QueryBuilder query = new BoolQueryBuilder().must(f);

        final Borrowed<Connection> c = connection().borrow();

        if (!c.isValid()) {
            throw new IllegalStateException("connection is not available");
        }

        return observer -> {
            final AtomicLong index = new AtomicLong();
            final SearchRequestBuilder builder;

            try {
                builder = c
                    .get()
                    .search(type)
                    .setSize(ENTRIES_SCAN_SIZE)
                    .setScroll(ENTRIES_TIMEOUT)
                    .setQuery(query);
            } catch (NoIndexSelectedException e) {
                throw new IllegalArgumentException("no valid index selected", e);
            }

            final OptionalLimit limit = request.getLimit();
            final AsyncObserver<Entries> o = observer.onFinished(c::release);

            bind(builder.execute()).onDone(new FutureDone<SearchResponse>() {
                @Override
                public void failed(Throwable cause) throws Exception {
                    o.fail(cause);
                }

                @Override
                public void cancelled() throws Exception {
                    o.cancel();
                }

                @Override
                public void resolved(final SearchResponse result) throws Exception {
                    final String scrollId = result.getScrollId();

                    if (scrollId == null) {
                        throw new RuntimeException("No scroll id associated with response");
                    }

                    handleNext(scrollId);
                }

                private void handleNext(final String scrollId) throws Exception {
                    if (limit.isGreater(index.get())) {
                        o.end();
                        return;
                    }

                    bind(c.get().prepareSearchScroll(scrollId).setScroll(ENTRIES_TIMEOUT).execute())
                        .onResolved(result -> {
                            final SearchHit[] hits = result.getHits().hits();

                            /* no more results */
                            if (hits.length == 0) {
                                o.end();
                                return;
                            }

                            final List<Series> entries = new ArrayList<>();

                            for (final SearchHit hit : hits) {
                                final long i = index.getAndIncrement();

                                if (limit.isGreater(i)) {
                                    break;
                                }

                                entries.add(toSeries(hit));
                            }

                            o
                                .observe(new Entries(entries))
                                .onResolved(v -> handleNext(scrollId))
                                .onFailed(o::fail)
                                .onCancelled(o::cancel);
                        })
                        .onFailed(o::fail)
                        .onCancelled(o::cancel);
                }
            });
        };
    }

    @Data
    public static class LimitedSet<T> {
        private final Set<T> set;
        private final boolean limited;

        public static <T> LimitedSet<T> of() {
            return new LimitedSet<>(ImmutableSet.of(), false);
        }
    }
}
