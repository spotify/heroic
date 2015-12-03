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

import java.util.Collection;
import java.util.List;

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Initializing;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;

import eu.toolchain.async.AsyncFuture;

public interface MetricBackend extends Initializing, Grouped {
    Statistics getStatistics();

    /**
     * Configure the metric backend.
     *
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
     * @throws MetricBackendException If the write cannot be performed.
     */
    AsyncFuture<WriteResult> write(WriteMetric write);

    /**
     * Write a collection of datapoints for a specific time series.
     *
     * @param series Time serie to write to.
     * @param data Datapoints to write.
     * @return A callback indicating if the write was successful or not.
     * @throws MetricBackendException If the write cannot be performed.
     */
    AsyncFuture<WriteResult> write(Collection<WriteMetric> writes);

    /**
     * Query for data points that is part of the specified list of rows and range.
     *
     * @param type The type of metric to fetch.
     * @param series The series to fetch metrics for.
     * @param range The range to fetch metrics for.
     * @param watcher The watcher implementation to use when fetching metrics.
     *
     * @return A future containing the fetched data wrapped in a {@link FetchData} structure.
     */
    AsyncFuture<FetchData> fetch(MetricType type, Series series, DateRange range,
            FetchQuotaWatcher watcher, QueryOptions options);

    /**
     * List all series directly from the database.
     *
     * This will be incredibly slow.
     *
     * @return An iterator over all found time series.
     * @throws MetricBackendException If listing of entries cannot be performed.
     */
    Iterable<BackendEntry> listEntries();

    /**
     * Return a list of all matching backend keys.
     *
     * @param filter If specified, limit results using the given clause.
     * @param options Query options to apply.
     * @return An observable providing a list of {@link BackendKey}s that can be observed for all
     *         matching keys.
     */
    AsyncObservable<BackendKeySet> streamKeys(BackendKeyFilter filter, QueryOptions options);

    /**
     * Stream keys with a high-level paging implementation.
     *
     * @param filter If specified, limit results using the given clause.
     * @param options Query options to apply.
     * @return An observable providing a list of {@link BackendKey}s that can be observed for all
     *         matching keys.
     */
    AsyncObservable<BackendKeySet> streamKeysPaged(BackendKeyFilter filter, QueryOptions options,
            int pageSize);

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
     *
     * This reduces max memory utilization required in comparison to {#link fetchRow(BackendKey)}.
     */
    AsyncObservable<MetricCollection> streamRow(BackendKey key);
}
