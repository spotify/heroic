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

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Initializing;
import com.spotify.heroic.common.Series;

import eu.toolchain.async.AsyncFuture;

public interface MetricBackend extends Initializing, Grouped {
    /**
     * Configure the metric backend.
     *
     * This will assert that all required tables exists and are configured correctly for the given backend.
     *
     * @return A future that will be resolved when the configuration is successfully completed.
     */
    public AsyncFuture<Void> configure();

    /**
     * Execute a single write.
     *
     * @param write
     * @return
     * @throws MetricBackendException
     *             If the write cannot be performed.
     */
    public AsyncFuture<WriteResult> write(WriteMetric write);

    /**
     * Write a collection of datapoints for a specific time series.
     *
     * @param series
     *            Time serie to write to.
     * @param data
     *            Datapoints to write.
     * @return A callback indicating if the write was successful or not.
     * @throws MetricBackendException
     *             If the write cannot be performed.
     */
    public AsyncFuture<WriteResult> write(Collection<WriteMetric> writes);

    /**
     * Query for data points that is part of the specified list of rows and range.
     *
     * @param type
     *            The type of metric to fetch.
     * @param series
     *            The series to fetch metrics for.
     * @param range
     *            The range to fetch metrics for.
     * @param watcher
     *            The watcher implementation to use when fetching metrics.
     *
     * @return A future containing the fetched data wrapped in a {@link FetchData} structure.
     */
    public AsyncFuture<FetchData> fetch(MetricType type, Series series, DateRange range,
            FetchQuotaWatcher watcher, QueryOptions options);

    /**
     * List all series directly from the database.
     *
     * This will be incredibly slow.
     *
     * @return An iterator over all found time series.
     * @throws MetricBackendException
     *             If listing of entries cannot be performed.
     */
    public Iterable<BackendEntry> listEntries();

    /**
     * Return a list of all matching backend keys.
     *
     * @param start
     *            If specified, limit start to this point.
     * @param end
     *            If specified, limit end to this point.
     * @param limit
     *            Limit the amount of results, max will always be 1000.
     * @return A future containing a list of backend keys.
     */
    public AsyncFuture<BackendKeySet> keys(BackendKey start, int limit, final QueryOptions options);

    /**
     * Serialize the given key, and return the hex-representation.
     * @return A list of all possible hex serialized keys.
     */
    public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key);

    /**
     * Serialize the given key, and return the corresponding BackendKey.
     */
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key);

    /**
     * Delete all data associated with the given key.
     */
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options);
}
