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
import com.google.common.base.Stopwatch;
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
import com.spotify.heroic.metric.MetricReadResult;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.statistics.DataInMemoryReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import eu.toolchain.async.AsyncFuture;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracing;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Consumer;

public class SemanticMetricBackendReporter implements MetricBackendReporter {
    private static final String COMPONENT = "metric-backend";

    private final FutureReporter write;
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
    private final Histogram queryReadRate;
    // Average milliseconds between samples
    private final Histogram queryRowMsBetweenSamples;
    // Average samples per mega-seconds :)
    private final Histogram queryRowDensity;
    // Counter of dropped writes due to row key size
    private final Counter writesDroppedBySize;

    public SemanticMetricBackendReporter(SemanticMetricRegistry registry) {
        final MetricId base = MetricId.build().tagged("component", COMPONENT);

        this.write =
            new SemanticFutureReporter(registry, base.tagged("what", "write", "unit", Units.WRITE));
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

        sampleSizeLive = registry.counter(
            base.tagged("what", "sample-size-live", "unit", Units.SAMPLE));
        sampleSizeAccumulated = registry.counter(
            base.tagged("what", "sample-size-accumulated", "unit", Units.SAMPLE));

        querySamplesRead = registry.histogram(
            base.tagged("what", "query-metrics-samples-read", "unit", Units.COUNT));
        queryRowsAccessed = registry.histogram(
            base.tagged("what", "query-metrics-rows-accessed", "unit", Units.COUNT));
        queryMaxLiveSamples = registry.histogram(
            base.tagged("what", "query-metrics-max-live-samples", "unit", Units.COUNT));
        queryReadRate = registry.histogram(
            base.tagged("what", "query-metrics-read-rate", "unit", Units.COUNT));
        queryRowMsBetweenSamples = registry.histogram(
            base.tagged("what", "query-metrics-row-metric-distance", "unit", Units.MILLISECOND));
        queryRowDensity = registry.histogram(
            base.tagged("what", "query-metrics-row-density", "unit", Units.COUNT));

        writesDroppedBySize = registry.counter(
            base.tagged("what", "writes-dropped-by-size", "unit", Units.COUNT));
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
            private final Stopwatch queryWatch = Stopwatch.createStarted();

            @Override
            public void reportRowsAccessed(final long n) {
                rowsAccessed.accumulate(n);
            }

            @Override
            public void reportRowDensity(final double samplesPerSecond) {
                queryRowDensity.update((long) (samplesPerSecond * 1e6));
                queryRowMsBetweenSamples.update((long) (1000.0 / samplesPerSecond));
            }

            @Override
            public void reportDataHasBeenRead(final long n) {
                dataRead.accumulate(n);

                final long currDataInMemory = dataInMemory.addAndGet(n);
                maxDataInMemory.accumulate(currDataInMemory);

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

                final long finalDataRead = dataRead.get();
                /* The accumulated count has previously been incremented with all of the data read,
                 * so decrease the full amount now */
                sampleSizeAccumulated.dec(finalDataRead);

                querySamplesRead.update(finalDataRead);
                queryRowsAccessed.update(rowsAccessed.get());
                queryMaxLiveSamples.update(maxDataInMemory.get());

                final long durationNs = queryWatch.elapsed(TimeUnit.NANOSECONDS);
                if (durationNs != 0) {
                    // Amount of data read per second
                    queryReadRate.update((1_000_000_000 * finalDataRead) / durationNs);
                }
            }
        };
    }

    @Override
    public void reportWritesDroppedBySize() {
        writesDroppedBySize.inc();
    }

    @Override
    public FutureReporter.Context reportFindSeries() {
        return findSeries.setup();
    }

    @Override
    public FutureReporter.Context reportQueryMetrics() {
        return queryMetrics.setup();
    }

    public String toString() {
        return "SemanticMetricBackendReporter()";
    }

    private class InstrumentedMetricBackend implements MetricBackend {
        private final MetricBackend delegate;

        @java.beans.ConstructorProperties({ "delegate" })
        public InstrumentedMetricBackend(final MetricBackend delegate) {
            this.delegate = delegate;
        }

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
            return write(request, Tracing.getTracer().getCurrentSpan());
        }

        @Override
        public AsyncFuture<WriteMetric> write(
            final WriteMetric.Request request, final Span parentSpan) {
            return delegate.write(request, parentSpan).onDone(write.setup());
        }

        @Override
        public AsyncFuture<FetchData.Result> fetch(
            final FetchData.Request request,
            final FetchQuotaWatcher watcher,
            final Consumer<MetricReadResult> metricsConsumer,
            final Span parentSpan
        ) {
            return delegate.fetch(
                request, watcher, metricsConsumer, parentSpan).onDone(fetch.setup());
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
