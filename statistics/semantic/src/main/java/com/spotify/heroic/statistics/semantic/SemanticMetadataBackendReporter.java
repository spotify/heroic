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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;

@ToString(of = {"base"})
public class SemanticMetadataBackendReporter implements MetadataBackendReporter {
    private static final String COMPONENT = "metadata-backend";

    private final FutureReporter findTags;
    private final FutureReporter findSeries;
    private final FutureReporter countSeries;
    private final FutureReporter deleteSeries;
    private final FutureReporter findKeys;
    private final FutureReporter write;

    private final Meter writeSuccess;
    private final Meter writeFailure;
    private final Meter entries;

    private final Meter writesDroppedByRateLimit;

    private final Histogram writeBatchDuration;

    public SemanticMetadataBackendReporter(SemanticMetricRegistry registry) {
        final MetricId base = MetricId.build().tagged("component", COMPONENT);

        findTags = new SemanticFutureReporter(registry,
            base.tagged("what", "find-tags", "unit", Units.QUERY));
        findSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "find-series", "unit", Units.QUERY));
        countSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "count-series", "unit", Units.QUERY));
        deleteSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "delete-series", "unit", Units.QUERY));
        findKeys = new SemanticFutureReporter(registry,
            base.tagged("what", "find-keys", "unit", Units.QUERY));
        write =
            new SemanticFutureReporter(registry, base.tagged("what", "write", "unit", Units.WRITE));
        writeSuccess = registry.meter(base.tagged("what", "write-success", "unit", Units.WRITE));
        writeFailure = registry.meter(base.tagged("what", "write-failure", "unit", Units.FAILURE));
        entries = registry.meter(base.tagged("what", "entries", "unit", Units.QUERY));

        writesDroppedByRateLimit =
            registry.meter(base.tagged("what", "writes-dropped-by-rate-limit", "unit", Units.DROP));

        writeBatchDuration = registry.histogram(
            base.tagged("what", "write-bulk-duration", "unit", Units.MILLISECOND));
    }

    @Override
    public MetadataBackend decorate(
        final MetadataBackend backend
    ) {
        return new InstrumentedMetadataBackend(backend);
    }

    @Override
    public void reportWriteDroppedByRateLimit() {
        writesDroppedByRateLimit.mark();
    }

    @Override
    public void reportWriteSuccess(long n) {
        writeSuccess.mark(n);
    }

    @Override
    public void reportWriteFailure(long n) {
        writeFailure.mark(n);
    }

    @Override
    public void reportWriteBatchDuration(long millis) {
        writeBatchDuration.update(millis);
    }

    @RequiredArgsConstructor
    class InstrumentedMetadataBackend implements MetadataBackend {
        private final MetadataBackend delegate;

        @Override
        public AsyncFuture<Void> configure() {
            return delegate.configure();
        }

        @Override
        public AsyncFuture<WriteResult> write(final Series series, final DateRange range) {
            return delegate.write(series, range).onDone(write.setup());
        }

        @Override
        public AsyncObservable<List<Series>> entries(final RangeFilter filter) {
            entries.mark();
            return delegate.entries(filter);
        }

        @Override
        public AsyncFuture<FindTags> findTags(final RangeFilter filter) {
            return delegate.findTags(filter).onDone(findTags.setup());
        }

        @Override
        public AsyncFuture<FindSeries> findSeries(final RangeFilter filter) {
            return delegate.findSeries(filter).onDone(findSeries.setup());
        }

        @Override
        public AsyncFuture<CountSeries> countSeries(final RangeFilter filter) {
            return delegate.countSeries(filter).onDone(countSeries.setup());
        }

        @Override
        public AsyncFuture<DeleteSeries> deleteSeries(final RangeFilter filter) {
            return delegate.deleteSeries(filter).onDone(deleteSeries.setup());
        }

        @Override
        public AsyncFuture<FindKeys> findKeys(final RangeFilter filter) {
            return delegate.findKeys(filter).onDone(findKeys.setup());
        }

        @Override
        public Statistics getStatistics() {
            return delegate.getStatistics();
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public Groups getGroups() {
            return delegate.getGroups();
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public String toString() {
            return delegate.toString() + "{semantic}";
        }
    }
}
