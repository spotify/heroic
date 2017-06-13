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

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Collected;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Initializing;
import com.spotify.heroic.common.Statistics;

import eu.toolchain.async.AsyncFuture;

import java.util.List;
import java.util.function.Consumer;

public interface MetricBackend extends Initializing, Grouped, Collected {
    Statistics getStatistics();

    /**
     * Configure the metric backend.
     * <p>
     * This will assert that all required tables exists and are configured correctly for the given
     * backend.
     *
     * @return A future that will be resolved when the configuration is successfully completed.
     */
    AsyncFuture<Void> configure();

    /**
     * Execute a single write.
     *
     * @param write
     * @return
     */
    AsyncFuture<WriteMetric> write(WriteMetric.Request write);

    /**
     * Query for data points that is part of the specified list of rows and range.
     *
     * @param request Fetch request to use.
     * @param watcher The watcher implementation to use when fetching metrics.
     * @return A future containing the fetched data wrapped in a {@link FetchData} structure.
     */
    @Deprecated
    AsyncFuture<FetchData> fetch(FetchData.Request request, FetchQuotaWatcher watcher);

    /**
     * Query for data points that is part of the specified list of rows and range.
     *
     * @param request Fetch request to use.
     * @param watcher The watcher implementation to use when fetching metrics.
     * @param metricsConsumer The consumer that receives the fetched data
     * @return A future containing the fetch result.
     */
    AsyncFuture<FetchData.Result> fetch(
        FetchData.Request request, FetchQuotaWatcher watcher,
        Consumer<MetricCollection> metricsConsumer
    );

    /**
     * List all series directly from the database.
     * <p>
     * This will be incredibly slow.
     *
     * @return An iterator over all found time series.
     */
    Iterable<BackendEntry> listEntries();

    /**
     * Return a list of all matching backend keys.
     *
     * @param filter If specified, limit results using the given clause.
     * @param options Query options to apply.
     * @return An observable providing a list of {@link BackendKey}s that can be observed for all
     * matching keys.
     */
    AsyncObservable<BackendKeySet> streamKeys(BackendKeyFilter filter, QueryOptions options);

    /**
     * Stream keys with a high-level paging implementation.
     *
     * @param filter If specified, limit results using the given clause.
     * @param options Query options to apply.
     * @return An observable providing a list of {@link BackendKey}s that can be observed for all
     * matching keys.
     */
    AsyncObservable<BackendKeySet> streamKeysPaged(
        BackendKeyFilter filter, QueryOptions options, long pageSize
    );

    /**
     * Serialize the given key, and return the hex-representation.
     *
     * @return A list of all possible hex serialized keys.
     */
    AsyncFuture<List<String>> serializeKeyToHex(BackendKey key);

    /**
     * Serialize the given key, and return the corresponding BackendKey.
     */
    AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key);

    /**
     * Delete all data associated with the given key.
     */
    AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options);

    /**
     * Count the number of data points for the given key.
     */
    AsyncFuture<Long> countKey(BackendKey key, QueryOptions options);

    /**
     * Fetch a complete row from the backend.
     */
    AsyncFuture<MetricCollection> fetchRow(BackendKey key);

    /**
     * Stream an entire row, chunk-by-chunk.
     * <p>
     * This reduces max memory utilization required in comparison to {#link fetchRow(BackendKey)}.
     */
    AsyncObservable<MetricCollection> streamRow(BackendKey key);
}
