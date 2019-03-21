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

package com.spotify.heroic.metric;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Statistics;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import java.util.Optional;

public abstract class AbstractMetricBackend implements MetricBackend {
    private final AsyncFramework async;

    public AbstractMetricBackend(final AsyncFramework async) {
        this.async = async;
    }

    @Override
    public Statistics getStatistics() {
        return Statistics.empty();
    }

    @Override
    public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
        return async.resolved(ImmutableList.of());
    }

    @Override
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
        return async.resolved(ImmutableList.of());
    }

    @Override
    public AsyncObservable<BackendKeySet> streamKeys(
        BackendKeyFilter filter, QueryOptions options
    ) {
        return AsyncObservable.empty();
    }

    @Override
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Long> countKey(BackendKey key, QueryOptions options) {
        return async.resolved(0L);
    }

    @Override
    public AsyncFuture<MetricCollection> fetchRow(BackendKey key) {
        return async.failed(new Exception("not supported"));
    }

    @Override
    public AsyncObservable<MetricCollection> streamRow(BackendKey key) {
        return AsyncObserver::end;
    }

    @Override
    public AsyncObservable<BackendKeySet> streamKeysPaged(
        BackendKeyFilter filter, final QueryOptions options, final long pageSize
    ) {
        return observer -> streamKeysNextPage(observer, filter, options, pageSize,
            Optional.empty());
    }

    private void streamKeysNextPage(
        final AsyncObserver<BackendKeySet> observer, final BackendKeyFilter filter,
        final QueryOptions options, final long pageSize, final Optional<BackendKey> key
    ) {
        final BackendKeyFilter partial = key
            .map(BackendKeyFilter::gt)
            .map(filter::withStart)
            .orElse(filter)
            .withLimit(OptionalLimit.of(pageSize));

        final AsyncObservable<BackendKeySet> observable = streamKeys(partial, options);

        observable.observe(new AsyncObserver<BackendKeySet>() {
            private BackendKey lastSeen = null;

            @Override
            public AsyncFuture<Void> observe(BackendKeySet value) {
                lastSeen = value.getKeys().get(value.getKeys().size() - 1);
                return observer.observe(value);
            }

            @Override
            public void cancel() {
                observer.cancel();
            }

            @Override
            public void fail(Throwable cause) {
                observer.fail(cause);
            }

            @Override
            public void end() {
                // no key seen, we are at the end of the sequence.
                if (lastSeen == null) {
                    observer.end();
                    return;
                }

                final BackendKey next = lastSeen;
                lastSeen = null;
                streamKeysNextPage(observer, filter, options, pageSize, Optional.of(next));
            }
        });
    }
}
