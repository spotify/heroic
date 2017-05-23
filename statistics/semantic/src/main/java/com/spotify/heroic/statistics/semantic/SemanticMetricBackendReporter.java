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

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.statistics.DataInMemoryReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {"base"})
public class SemanticMetricBackendReporter implements MetricBackendReporter {
    private static final String COMPONENT = "metric-backend";

    private final FutureReporter write;
    private final FutureReporter writeBatch;
    private final FutureReporter fetch;
    private final FutureReporter deleteKey;
    private final FutureReporter countKey;
    private final FutureReporter fetchRow;

    private final FutureReporter findSeries;
    private final FutureReporter queryMetrics;

    /* Total amount of data points in memory at any given time, across all queries */
    private final Counter sampleSizeLive;

    /* Total amount of data points in memory + data points that has earlier been read in all ongoing
     * queries. This gives an upper bound, of sorts, on amount of active data points we have. */
    private final Counter sampleSizeAccumulated;

    /* Measuring on a data-node level, can be used for measuring load on metric backend */
    private final Histogram querySamplesRead;
    private final Histogram queryRowsAccessed;
    /* Maximum amount of data points in memory at any time for a query */
    private final Histogram queryMaxLiveSamples;

    public SemanticMetricBackendReporter(SemanticMetricRegistry registry) {
        final MetricId base = MetricId.build().tagged("component", COMPONENT);

        this.write =
            new SemanticFutureReporter(registry, base.tagged("what", "write", "unit", Units.WRITE));
        this.writeBatch = new SemanticFutureReporter(registry,
            base.tagged("what", "write-batch", "unit", Units.WRITE));
        this.fetch =
            new SemanticFutureReporter(registry, base.tagged("what", "fetch", "unit", Units.QUERY));
        this.deleteKey = new SemanticFutureReporter(registry,
            base.tagged("what", "delete-key", "unit", Units.DELETE));
        this.countKey = new SemanticFutureReporter(registry,
            base.tagged("what", "count-key", "unit", Units.QUERY));
        this.fetchRow = new SemanticFutureReporter(registry,
            base.tagged("what", "fetch-row", "unit", Units.QUERY));

        this.findSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "find-series", "unit", Units.QUERY));
        this.queryMetrics = new SemanticFutureReporter(registry,
            base.tagged("what", "query-metrics", "unit", Units.QUERY));

        sampleSizeLive =
            registry.counter(base.tagged("what", "sample-size-live", "unit", Units.SAMPLE));
        sampleSizeAccumulated =
            registry.counter(base.tagged("what", "sample-size-accumulated", "unit", Units.SAMPLE));

        querySamplesRead = registry.histogram(
            base.tagged("what", "query-metrics-samples-read", "unit", Units.COUNT));
        queryRowsAccessed = registry.histogram(
            base.tagged("what", "query-metrics-rows-accessed", "unit", Units.COUNT));
        queryMaxLiveSamples = registry.histogram(
            base.tagged("what", "query-metrics-max-live-samples", "unit", Units.COUNT));
    }

    @Override
    public MetricBackend decorate(final MetricBackend backend) {
        return new InstrumentedMetricBackend(backend);
    }

    @Override
    public DataInMemoryReporter newDataInMemoryReporter() {
        return new DataInMemoryReporter() {
            private final LongAccumulator dataRead = new LongAccumulator((x, y) -> x + y, 0L);
            private final AtomicLong dataInMemory = new AtomicLong(0L);
            private final LongAccumulator rowsAccessed = new LongAccumulator((x, y) -> x + y, 0L);
            private final LongAccumulator maxDataInMemory = new LongAccumulator(Long::max, 0L);

            @Override
            public void reportRowsAccessed(final long n) {
                rowsAccessed.accumulate(n);
            }

            @Override
            public void reportDataHasBeenRead(final long n) {
                dataRead.accumulate(n);

                maxDataInMemory.accumulate(dataInMemory.addAndGet(n));

                sampleSizeLive.inc(n);
                sampleSizeAccumulated.inc(n);
            }

            @Override
            public void reportDataNoLongerNeeded(final long n) {
                dataInMemory.addAndGet(-n);

                sampleSizeLive.dec(n);
            }

            @Override
            public void reportOperationEnded() {
                // Decrement any remaining live memory
                sampleSizeLive.dec(dataInMemory.get());
                /* The accumulated count has previously been incremented with all of the data read,
                 * so decrease the full amount now */
                sampleSizeAccumulated.dec(dataRead.get());

                querySamplesRead.update(dataRead.get());
                queryRowsAccessed.update(rowsAccessed.get());
                queryMaxLiveSamples.update(maxDataInMemory.get());
            }
        };
    }

    @Override
    public FutureReporter.Context reportFindSeries() {
        return findSeries.setup();
    }

    @Override
    public FutureReporter.Context reportQueryMetrics() {
        return queryMetrics.setup();
    }

    @RequiredArgsConstructor
    private class InstrumentedMetricBackend implements MetricBackend {
        private final MetricBackend delegate;

        @Override
        public Statistics getStatistics() {
            return delegate.getStatistics();
        }

        @Override
        public AsyncFuture<Void> configure() {
            return delegate.configure();
        }

        @Override
        public AsyncFuture<WriteMetric> write(final WriteMetric.Request request) {
            return delegate.write(request).onDone(write.setup());
        }

        @Override
        public AsyncFuture<FetchData> fetch(
            final FetchData.Request request, final FetchQuotaWatcher watcher
        ) {
            return delegate.fetch(request, watcher).onDone(fetch.setup());
        }

        @Override
        public AsyncFuture<FetchData.Result> fetch(
            final FetchData.Request request, final FetchQuotaWatcher watcher,
            final Consumer<MetricCollection> metricsConsumer
        ) {
            return delegate.fetch(request, watcher, metricsConsumer).onDone(fetch.setup());
        }

        @Override
        public Iterable<BackendEntry> listEntries() {
            return delegate.listEntries();
        }

        @Override
        public AsyncObservable<BackendKeySet> streamKeys(
            final BackendKeyFilter filter, final QueryOptions options
        ) {
            return delegate.streamKeys(filter, options);
        }

        @Override
        public AsyncObservable<BackendKeySet> streamKeysPaged(
            final BackendKeyFilter filter, final QueryOptions options, final long pageSize
        ) {
            return delegate.streamKeysPaged(filter, options, pageSize);
        }

        @Override
        public AsyncFuture<List<String>> serializeKeyToHex(
            final BackendKey key
        ) {
            return delegate.serializeKeyToHex(key);
        }

        @Override
        public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(final String key) {
            return delegate.deserializeKeyFromHex(key);
        }

        @Override
        public AsyncFuture<Void> deleteKey(final BackendKey key, final QueryOptions options) {
            return delegate.deleteKey(key, options).onDone(deleteKey.setup());
        }

        @Override
        public AsyncFuture<Long> countKey(final BackendKey key, final QueryOptions options) {
            return delegate.countKey(key, options).onDone(countKey.setup());
        }

        @Override
        public AsyncFuture<MetricCollection> fetchRow(final BackendKey key) {
            return delegate.fetchRow(key).onDone(fetchRow.setup());
        }

        @Override
        public AsyncObservable<MetricCollection> streamRow(final BackendKey key) {
            return delegate.streamRow(key);
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public Groups groups() {
            return delegate.groups();
        }

        @Override
        public String toString() {
            return delegate.toString() + "{semantic}";
        }
    }
}
