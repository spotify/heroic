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

import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Managed;
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

    protected AsyncFuture<FindSeries> scrollOverSeries(
        final Connection c, final SearchRequestBuilder request, final long limit
    ) {
        return bind(request.execute()).lazyTransform((initial) -> {
            if (initial.getScrollId() == null) {
                return async.resolved(FindSeries.EMPTY);
            }

            final String scrollId = initial.getScrollId();

            final Supplier<AsyncFuture<SearchResponse>> scroller =
                () -> bind(c.prepareSearchScroll(scrollId).setScroll(SCROLL_TIME).execute());

            return scroller.get().lazyTransform(new ScrollTransform(limit, scroller));
        });
    }

    @RequiredArgsConstructor
    class ScrollTransform implements LazyTransform<SearchResponse, FindSeries> {
        private final long limit;
        private final Supplier<AsyncFuture<SearchResponse>> scroller;

        int size = 0;
        int duplicates = 0;
        final Set<Series> series = new HashSet<>();

        @Override
        public AsyncFuture<FindSeries> transform(final SearchResponse response) throws Exception {
            final SearchHit[] hits = response.getHits().getHits();

            for (final SearchHit hit : hits) {
                if (size >= limit) {
                    break;
                }

                if (!series.add(toSeries(hit))) {
                    duplicates += 1;
                }

                size += 1;
            }

            if (hits.length == 0 || size >= limit) {
                return async.resolved(new FindSeries(series, size, duplicates));
            }

            return scroller.get().lazyTransform(this);
        }
    }

    private static final int ENTRIES_SCAN_SIZE = 1000;
    private static final TimeValue ENTRIES_TIMEOUT = TimeValue.timeValueSeconds(5);

    @Override
    public AsyncObservable<List<Series>> entries(final RangeFilter filter) {
        final FilterBuilder f = filter(filter.getFilter());

        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f);

        final Borrowed<Connection> c = connection().borrow();

        if (!c.isValid()) {
            throw new IllegalStateException("connection is not available");
        }

        return observer -> {
            final AtomicLong index = new AtomicLong();
            final long limit = filter.getLimit();
            final SearchRequestBuilder request;

            try {
                request = c
                    .get()
                    .search(filter.getRange(), type)
                    .setSize(ENTRIES_SCAN_SIZE)
                    .setScroll(ENTRIES_TIMEOUT)
                    .setSearchType(SearchType.SCAN)
                    .setQuery(query);
            } catch (NoIndexSelectedException e) {
                throw new IllegalArgumentException("no valid index selected", e);
            }

            final AsyncObserver<List<Series>> o = observer.onFinished(c::release);

            bind(request.execute()).onDone(new FutureDone<SearchResponse>() {
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
                    if (index.get() >= limit) {
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

                                if (i >= limit) {
                                    break;
                                }

                                entries.add(toSeries(hit));
                            }

                            o
                                .observe(entries)
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
}
