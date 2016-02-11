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
import com.spotify.heroic.common.Statistics;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public abstract class AbstractMetricBackend implements MetricBackend {
    private final AsyncFramework async;

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
        return observer -> {
            observer.end();
        };
    }

    @Override
    public AsyncObservable<BackendKeySet> streamKeysPaged(
        BackendKeyFilter filter, final QueryOptions options, final int pageSize
    ) {
        return observer -> {
            streamKeysNextPage(observer, filter, options, pageSize, null);
        };
    }

    private void streamKeysNextPage(
        final AsyncObserver<BackendKeySet> observer, final BackendKeyFilter filter,
        final QueryOptions options, final int pageSize, final BackendKey key
    ) throws Exception {
        BackendKeyFilter partial = filter;

        if (key != null) {
            partial = filter.withStart(BackendKeyFilter.gt(key));
        }

        partial = partial.withLimit(pageSize);

        final AsyncObservable<BackendKeySet> observable = streamKeys(partial, options);

        observable.observe(new AsyncObserver<BackendKeySet>() {
            private BackendKey lastSeen = null;

            @Override
            public AsyncFuture<Void> observe(BackendKeySet value) throws Exception {
                lastSeen = value.getKeys().get(value.getKeys().size() - 1);
                return observer.observe(value);
            }

            @Override
            public void cancel() throws Exception {
                observer.cancel();
            }

            @Override
            public void fail(Throwable cause) throws Exception {
                observer.fail(cause);
            }

            @Override
            public void end() throws Exception {
                // no key seen, we are at the end of the sequence.
                if (lastSeen == null) {
                    observer.end();
                    return;
                }

                final BackendKey next = lastSeen;
                lastSeen = null;
                streamKeysNextPage(observer, filter, options, pageSize, next);
            }
        });
    }
}
