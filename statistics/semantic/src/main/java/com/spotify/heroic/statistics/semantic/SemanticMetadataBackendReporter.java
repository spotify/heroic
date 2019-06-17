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
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.Entries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindSeriesIds;
import com.spotify.heroic.metadata.FindSeriesIdsStream;
import com.spotify.heroic.metadata.FindSeriesStream;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import eu.toolchain.async.AsyncFuture;

public class SemanticMetadataBackendReporter implements MetadataBackendReporter {
    private static final String COMPONENT = "metadata-backend";

    private final FutureReporter findTags;
    private final FutureReporter findSeries;
    private final FutureReporter findSeriesIds;
    private final FutureReporter countSeries;
    private final FutureReporter deleteSeries;
    private final FutureReporter findKeys;
    private final FutureReporter write;
    private final FutureReporter backendWrite;

    private final Counter entries;
    private final Counter writesDroppedByCacheHit;
    private final Counter writesDroppedByDuplicate;


    public SemanticMetadataBackendReporter(SemanticMetricRegistry registry) {
        final MetricId base = MetricId.build().tagged("component", COMPONENT);

        findTags = new SemanticFutureReporter(registry,
            base.tagged("what", "find-tags", "unit", Units.QUERY));
        findSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "find-series", "unit", Units.QUERY));
        findSeriesIds = new SemanticFutureReporter(registry,
            base.tagged("what", "find-series-ids", "unit", Units.QUERY));
        countSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "count-series", "unit", Units.QUERY));
        deleteSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "delete-series", "unit", Units.QUERY));
        findKeys = new SemanticFutureReporter(registry,
            base.tagged("what", "find-keys", "unit", Units.QUERY));
        write =
            new SemanticFutureReporter(registry, base.tagged("what", "write", "unit", Units.WRITE));
        backendWrite = new SemanticFutureReporter(registry,
            base.tagged("what", "backend-write", "unit", Units.WRITE));
        entries = registry.counter(base.tagged("what", "entries", "unit", Units.COUNT));

        writesDroppedByCacheHit = registry.counter(
            base.tagged("what", "writes-dropped-by-cache-hit", "unit", Units.COUNT));
        writesDroppedByDuplicate = registry.counter(
            base.tagged("what", "writes-dropped-by-duplicate", "unit", Units.COUNT));

    }

    @Override
    public MetadataBackend decorate(
        final MetadataBackend backend
    ) {
        return new InstrumentedMetadataBackend(backend);
    }

    @Override
    public FutureReporter.Context setupBackendWriteReporter() {
        return backendWrite.setup();
    }

    @Override
    public void reportWriteDroppedByCacheHit() {
        writesDroppedByCacheHit.inc();
    }

    @Override
    public void reportWriteDroppedByDuplicate() {
        writesDroppedByDuplicate.inc();
    }

    public String toString() {
        return "SemanticMetadataBackendReporter()";
    }

    class InstrumentedMetadataBackend implements MetadataBackend {
        private final MetadataBackend delegate;

        @java.beans.ConstructorProperties({ "delegate" })
        public InstrumentedMetadataBackend(final MetadataBackend delegate) {
            this.delegate = delegate;
        }

        @Override
        public AsyncFuture<Void> configure() {
            return delegate.configure();
        }

        @Override
        public AsyncFuture<WriteMetadata> write(final WriteMetadata.Request request) {
            return delegate.write(request).onDone(write.setup());
        }

        @Override
        public AsyncObservable<Entries> entries(final Entries.Request request) {
            entries.inc();
            return delegate.entries(request);
        }

        @Override
        public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
            return delegate.findTags(request).onDone(findTags.setup());
        }

        @Override
        public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
            return delegate.findSeries(request).onDone(findSeries.setup());
        }

        @Override
        public AsyncObservable<FindSeriesStream> findSeriesStream(
            final FindSeries.Request request
        ) {
            return delegate.findSeriesStream(request);
        }

        @Override
        public AsyncFuture<FindSeriesIds> findSeriesIds(
            final FindSeriesIds.Request request
        ) {
            return delegate.findSeriesIds(request).onDone(findSeriesIds.setup());
        }

        @Override
        public AsyncObservable<FindSeriesIdsStream> findSeriesIdsStream(
            final FindSeriesIds.Request request
        ) {
            return delegate.findSeriesIdsStream(request);
        }

        @Override
        public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
            return delegate.countSeries(request).onDone(countSeries.setup());
        }

        @Override
        public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
            return delegate.deleteSeries(request).onDone(deleteSeries.setup());
        }

        @Override
        public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
            return delegate.findKeys(request).onDone(findKeys.setup());
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
        public Groups groups() {
            return delegate.groups();
        }

        @Override
        public String toString() {
            return delegate.toString() + "{semantic}";
        }
    }
}
