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

package com.spotify.heroic.ingestion;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import com.spotify.heroic.common.BackendGroupException;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

public class IngestionManagerImpl implements IngestionManager {
    @Inject
    protected AsyncFramework async;

    @Inject
    protected MetadataManager metadata;

    @Inject
    protected MetricManager metric;

    @Inject
    protected SuggestManager suggest;

    @Inject
    protected IngestionManagerReporter reporter;

    private final boolean updateMetrics;
    private final boolean updateMetadata;
    private final boolean updateSuggestions;

    /**
     * @param updateMetrics
     *            Ingested metrics will update metric backends.
     * @param updateMetadata
     *            Ingested metrics will update metadata backends.
     * @param updateSuggestions
     *            Ingested metrics will update suggest backends.
     */
    public IngestionManagerImpl(final boolean updateMetrics, final boolean updateMetadata,
            final boolean updateSuggestions) {
        this.updateMetrics = updateMetrics;
        this.updateMetadata = updateMetadata;
        this.updateSuggestions = updateSuggestions;
    }

    final Transform<WriteResult, Void> metricTransform = new Transform<WriteResult, Void>() {
        @Override
        public Void transform(WriteResult result) throws Exception {
            return null;
        }
    };

    final Transform<Void, Void> metadataTransform = new Transform<Void, Void>() {
        @Override
        public Void transform(Void result) throws Exception {
            return null;
        }
    };

    // handle cancelled writes gracefully.
    private static final Transform<Void, WriteResult> HANDLE_CANCELLED = new Transform<Void, WriteResult>() {
        @Override
        public WriteResult transform(Void result) throws Exception {
            return WriteResult.EMPTY;
        }
    };

    @Override
    public AsyncFuture<WriteResult> write(final String group, final WriteMetric write) throws BackendGroupException {
        if (write.isEmpty())
            return async.resolved(WriteResult.of());

        return doWrite(group, write);
    }

    protected AsyncFuture<WriteResult> doWrite(final String group, final WriteMetric write)
            throws BackendGroupException {
        final MetricBackend metric = updateMetrics ? this.metric.useGroup(group) : null;
        final MetadataBackend metadata = updateMetadata ? this.metadata.useGroup(group) : null;
        final SuggestBackend suggest = updateSuggestions ? this.suggest.useGroup(group) : null;

        final List<AsyncFuture<WriteResult>> futures = new ArrayList<>();

        if (metric != null) {
            futures.add(doMetricWrite(write, metric));
        }

        if (metadata != null || suggest != null) {
            futures.add(doMetadataWrite(write, metadata, suggest));
        }

        return async.collect(futures, WriteResult.merger());
    }

    protected AsyncFuture<WriteResult> doMetricWrite(WriteMetric write, MetricBackend metric) {
        try {
            return metric.write(write);
        } catch (Exception e) {
            return async.<WriteResult> failed(e);
        }
    }

    protected AsyncFuture<WriteResult> doMetadataWrite(WriteMetric write, MetadataBackend metadata,
            SuggestBackend suggest) {
        // @formatter:off
        /**
         * These futures are in the following order;
         * collect(...):
         *   Since there are two writes, we merge them into one using the WriteResult.merger().
         * on(...):
         *   Measure the time it takes from this invocation, to the future being finished.
         *   This uses the FutureReporter.Context which extends FutureDone<Object> so that it can be
         *   notified for all different types of events (resolved, failed, cancelled) and reports a
         *   meter for each outcome.
         * cancelled(...):
         *   Transforms a cancelled future into a resolved one, using {@code HANDLE_CANCELLED}.
         *   This is a sort of 'catch' for futures which are cancelled, that notices that state,
         *   and transforms the cancellation into a usable value. In this case, an empty WriteResult.
         */
        return async
          .collect(doWriteMetadataMerged(write, metadata, suggest), WriteResult.merger())
          .onDone(reporter.reportMetadataWrite())
          .catchCancelled(HANDLE_CANCELLED);
        // @formatter:on
    }

    /**
     * Execute the given write on both the available metadata, and suggest backends.
     *
     * @param write
     *            The write to perform.
     * @param metadata
     *            The metadata backend to write to.
     * @param suggest
     *            The suggest backend to write to.
     * @return An array containing futures for all writes that occured.
     */
    protected List<AsyncFuture<WriteResult>> doWriteMetadataMerged(final WriteMetric write,
            final MetadataBackend metadata, final SuggestBackend suggest) {
        final DateRange range = rangeFrom(write);

        final List<AsyncFuture<WriteResult>> futures = new ArrayList<>();

        if (metadata != null) {
            try {
                futures.add(metadata.write(write.getSeries(), range));
            } catch (Exception e) {
                futures.add(async.<WriteResult> failed(e));
            }
        }

        if (suggest != null) {
            try {
                futures.add(suggest.write(write.getSeries(), range));
            } catch (Exception e) {
                futures.add(async.<WriteResult> failed(e));
            }
        }

        return futures;
    }

    private DateRange rangeFrom(WriteMetric write) {
        final Iterator<Metric> iterator = write.all().iterator();

        if (!iterator.hasNext())
            throw new IllegalArgumentException("write batch must not be empty");

        final Metric first = iterator.next();

        long start = first.getTimestamp();
        long end = first.getTimestamp();

        while (iterator.hasNext()) {
            final Metric d = iterator.next();
            start = Math.min(d.getTimestamp(), start);
            end = Math.max(d.getTimestamp(), end);
        }

        return new DateRange(start, end);
    }
}
