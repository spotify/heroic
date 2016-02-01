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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ResolvableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractElasticsearchMetadataBackend implements MetadataBackend {
    public static final TimeValue SCROLL_TIME = TimeValue.timeValueSeconds(5);

    private final AsyncFramework async;
    private final String type;

    protected abstract Managed<Connection> connection();

    protected abstract FilterBuilder filter(Filter filter);

    protected abstract Series toSeries(SearchHit hit);

    protected <T> AsyncFuture<T> bind(final ListenableActionFuture<T> actionFuture) {
        final ResolvableFuture<T> future = async.future();

        actionFuture.addListener(new ActionListener<T>() {
            @Override
            public void onResponse(T result) {
                future.resolve(result);
            }

            @Override
            public void onFailure(Throwable e) {
                future.fail(e);
            }
        });

        return future;
    }

    protected AsyncFuture<FindSeries> scrollOverSeries(final Connection c,
            final SearchRequestBuilder request, final long limit) {
        return bind(request.execute()).lazyTransform((initial) -> {
            if (initial.getScrollId() == null) {
                return async.resolved(FindSeries.EMPTY);
            }

            return bind(
                    c.prepareSearchScroll(initial.getScrollId()).setScroll(SCROLL_TIME).execute())
                            .lazyTransform((response) -> {
                final ResolvableFuture<FindSeries> future = async.future();
                final Set<Series> series = new HashSet<>();
                final AtomicInteger count = new AtomicInteger();

                final Consumer<SearchResponse> consumer = new Consumer<SearchResponse>() {
                    @Override
                    public void accept(final SearchResponse response) {
                        final SearchHit[] hits = response.getHits().hits();

                        for (final SearchHit hit : hits) {
                            series.add(toSeries(hit));
                        }

                        count.addAndGet(hits.length);

                        if (hits.length == 0 || count.get() >= limit
                                || response.getScrollId() == null) {
                            future.resolve(new FindSeries(series, series.size(), 0));
                            return;
                        }

                        bind(c.prepareSearchScroll(response.getScrollId()).setScroll(SCROLL_TIME)
                                .execute()).onDone(new FutureDone<SearchResponse>() {
                            @Override
                            public void failed(Throwable cause) throws Exception {
                                future.fail(cause);
                            }

                            @Override
                            public void resolved(SearchResponse result) throws Exception {
                                accept(result);
                            }

                            @Override
                            public void cancelled() throws Exception {
                                future.cancel();
                            }
                        });
                    }
                };

                consumer.accept(response);

                return future;
            });
        });
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
                request = c.get().search(filter.getRange(), type).setSize(ENTRIES_SCAN_SIZE)
                        .setScroll(ENTRIES_TIMEOUT).setSearchType(SearchType.SCAN).setQuery(query);
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

                        o.observe(entries).onResolved(v -> handleNext(scrollId)).onFailed(o::fail)
                                .onCancelled(o::cancel);
                    }).onFailed(o::fail).onCancelled(o::cancel);
                }
            });
        };
    }
}
