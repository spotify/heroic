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

package com.spotify.heroic.analytics.bigtable;

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;

import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@RequiredArgsConstructor
class BigtableAnalyticsMetricBackend implements MetricBackend {
    private final BigtableMetricAnalytics analytics;
    private final MetricBackend backend;

    @Override
    public boolean isReady() {
        return backend.isReady();
    }

    @Override
    public Groups getGroups() {
        return backend.getGroups();
    }

    @Override
    public Statistics getStatistics() {
        return backend.getStatistics();
    }

    @Override
    public AsyncFuture<Void> configure() {
        return backend.configure();
    }

    @Override
    public AsyncFuture<WriteResult> write(WriteMetric write) {
        return backend.write(write);
    }

    @Override
    public AsyncFuture<WriteResult> write(Collection<WriteMetric> writes) {
        return backend.write(writes);
    }

    @Override
    public AsyncFuture<FetchData> fetch(MetricType type, Series series, DateRange range,
            FetchQuotaWatcher watcher, QueryOptions options) {
        analytics.reportFetchSeries(LocalDate.now(), series);
        return backend.fetch(type, series, range, watcher, options);
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return backend.listEntries();
    }

    @Override
    public AsyncObservable<BackendKeySet> streamKeys(BackendKeyFilter filter,
            QueryOptions options) {
        return backend.streamKeys(filter, options);
    }

    @Override
    public AsyncObservable<BackendKeySet> streamKeysPaged(BackendKeyFilter filter,
            QueryOptions options, int pageSize) {
        return backend.streamKeysPaged(filter, options, pageSize);
    }

    @Override
    public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
        return backend.serializeKeyToHex(key);
    }

    @Override
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
        return backend.deserializeKeyFromHex(key);
    }

    @Override
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
        return backend.deleteKey(key, options);
    }

    @Override
    public AsyncFuture<Long> countKey(BackendKey key, QueryOptions options) {
        return backend.countKey(key, options);
    }

    @Override
    public AsyncFuture<MetricCollection> fetchRow(BackendKey key) {
        return backend.fetchRow(key);
    }

    @Override
    public AsyncObservable<MetricCollection> streamRow(BackendKey key) {
        return backend.streamRow(key);
    }
}
