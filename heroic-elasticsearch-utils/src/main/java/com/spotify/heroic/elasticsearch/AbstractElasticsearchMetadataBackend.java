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
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractElasticsearchMetadataBackend extends AbstractElasticsearchBackend
    implements MetadataBackend {
    public static final TimeValue SCROLL_TIME = TimeValue.timeValueSeconds(5);

    private final String type;

    public AbstractElasticsearchMetadataBackend(final AsyncFramework async, final String type) {
        super(async);
        this.type = type;
    }

    protected abstract Managed<Connection> connection();

    protected abstract FilterBuilder filter(Filter filter);

    protected abstract Series toSeries(SearchHit hit);

    protected <T> AsyncFuture<LimitedSet<T>> scrollEntries(
        final Connection c, final SearchRequestBuilder request, final OptionalLimit limit,
        final Function<SearchHit, T> converter
    ) {
        return bind(request.execute()).lazyTransform((initial) -> {
            if (initial.getScrollId() == null) {
                return async.resolved(LimitedSet.of());
            }

            final String scrollId = initial.getScrollId();

            final Supplier<AsyncFuture<SearchResponse>> scroller =
                () -> bind(c.prepareSearchScroll(scrollId).setScroll(SCROLL_TIME).execute());

            return scroller.get().lazyTransform(new ScrollTransform<>(limit, scroller, converter));
        });
    }

    @RequiredArgsConstructor
    class ScrollTransform<T> implements LazyTransform<SearchResponse, LimitedSet<T>> {
        private final OptionalLimit limit;
        private final Supplier<AsyncFuture<SearchResponse>> scroller;

        int size = 0;
        int duplicates = 0;
        final Set<T> results = new HashSet<>();
        final Function<SearchHit, T> converter;

        @Override
        public AsyncFuture<LimitedSet<T>> transform(final SearchResponse response)
            throws Exception {
            final SearchHit[] hits = response.getHits().getHits();

            for (final SearchHit hit : hits) {
                if (limit.isGreater(size)) {
                    return async.resolved(new LimitedSet<>(limit.limitSet(results), true));
                }

                if (!results.add(converter.apply(hit))) {
                    duplicates += 1;
                }

                size += 1;
            }

            if (hits.length == 0) {
                return async.resolved(new LimitedSet<>(results, false));
            }

            return scroller.get().lazyTransform(this);
        }
    }

    @RequiredArgsConstructor
    public class ScrollTransformStream<T> implements LazyTransform<SearchResponse, Void> {
        private final OptionalLimit limit;
        private final Supplier<AsyncFuture<SearchResponse>> scroller;
        private final Function<Set<T>, AsyncFuture<Void>> seriesFunction;
        private final Function<SearchHit, T> converter;

        int size = 0;

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

            return seriesFunction
                .apply(batch)
                .lazyTransform(v -> scroller.get().lazyTransform(this));
        }
    }

    private static final int ENTRIES_SCAN_SIZE = 1000;
    private static final TimeValue ENTRIES_TIMEOUT = TimeValue.timeValueSeconds(5);

    @Override
    public AsyncObservable<Entries> entries(final Entries.Request request) {
        final FilterBuilder f = filter(request.getFilter());

        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f);

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
                    .search(request.getRange(), type)
                    .setSize(ENTRIES_SCAN_SIZE)
                    .setScroll(ENTRIES_TIMEOUT)
                    .setSearchType(SearchType.SCAN)
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
