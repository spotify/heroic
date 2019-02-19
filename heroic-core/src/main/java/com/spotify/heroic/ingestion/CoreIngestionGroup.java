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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Collected;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.WriteSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

public class CoreIngestionGroup implements IngestionGroup {
    private final Tracer tracer = Tracing.getTracer();

    private final AsyncFramework async;
    private final Supplier<Filter> filter;
    private final Semaphore writePermits;
    private final IngestionManagerReporter reporter;
    private final LongAdder ingested;

    private final Optional<MetricBackend> metric;
    private final Optional<MetadataBackend> metadata;
    private final Optional<SuggestBackend> suggest;

    public CoreIngestionGroup(final AsyncFramework async,
                              final Supplier<Filter> filter,
                              final Semaphore writePermits,
                              final IngestionManagerReporter reporter,
                              final LongAdder ingested,
                              final Optional<MetricBackend> metric,
                              final Optional<MetadataBackend> metadata,
                              final Optional<SuggestBackend> suggest) {
        this.async = async;
        this.filter = filter;
        this.writePermits = writePermits;
        this.reporter = reporter;
        this.ingested = ingested;
        this.metric = metric;
        this.metadata = metadata;
        this.suggest = suggest;
    }

    @Override
    public Groups groups() {
        return Groups.combine(metric.map(Grouped::groups).orElseGet(Groups::empty),
            metadata.map(Grouped::groups).orElseGet(Groups::empty),
            suggest.map(Grouped::groups).orElseGet(Groups::empty));
    }

    @Override
    public AsyncFuture<Ingestion> write(final Ingestion.Request request) {
        ingested.increment();
        return syncWrite(request);
    }

    @Override
    public boolean isEmpty() {
        return metric.map(Collected::isEmpty).orElse(true) &&
            metadata.map(Collected::isEmpty).orElse(true) &&
            suggest.map(Collected::isEmpty).orElse(true);
    }

    protected AsyncFuture<Ingestion> syncWrite(final Ingestion.Request request) {
        final Span span = tracer.spanBuilder("CoreIngestionGroup.syncWrite").startSpan();

        if (!filter.get().apply(request.getSeries())) {
            reporter.reportDroppedByFilter();
            span.setStatus(Status.FAILED_PRECONDITION.withDescription("Dropped by filter"));
            span.end();
            return async.resolved(Ingestion.of(ImmutableList.of()));
        }

        try {
            span.addAnnotation("Acquiring write lock");
            writePermits.acquire();
        } catch (final InterruptedException e) {
            String error = "Failed to acquire semaphore for bounded request";
            span.setStatus(Status.INTERNAL.withDescription(error));
            span.end();
            return async.failed(new Exception(error, e));
        }

        span.addAnnotation("Acquired write lock");
        reporter.incrementConcurrentWrites();

        try (Scope ws = tracer.withSpan(span)) {
            return doWrite(request).onFinished(() -> {
                writePermits.release();
                reporter.decrementConcurrentWrites();
                span.end();
            });
        }
    }

    protected AsyncFuture<Ingestion> doWrite(final Ingestion.Request request) {
        final Span span = tracer.spanBuilder("CoreIngestionGroup.doWrite").startSpan();
        final List<AsyncFuture<Ingestion>> futures = new ArrayList<>();

        final Supplier<DateRange> range = rangeSupplier(request);

        metric.map(m -> doMetricWrite(m, request, span)).ifPresent(futures::add);
        metadata.map(m -> doMetadataWrite(m, request, range.get(), span))
            .ifPresent(futures::add);
        suggest.map(s -> doSuggestWrite(s, request, range.get(), span)).ifPresent(futures::add);

        return async.collect(futures, Ingestion.reduce()).onFinished(span::end);
    }

    protected AsyncFuture<Ingestion> doMetricWrite(
        final MetricBackend metric, final Ingestion.Request write, final Span parentSpan
    ) {
        return metric
            .write(new WriteMetric.Request(write.getSeries(), write.getData()), parentSpan)
            .directTransform(Ingestion::fromWriteMetric);
    }

    protected AsyncFuture<Ingestion> doMetadataWrite(
        final MetadataBackend metadata, final Ingestion.Request write, final DateRange range,
        final Span parentSpan
    ) {
        return metadata
            .write(new WriteMetadata.Request(write.getSeries(), range), parentSpan)
            .directTransform(Ingestion::fromWriteMetadata);
    }

    protected AsyncFuture<Ingestion> doSuggestWrite(
        final SuggestBackend suggest, final Ingestion.Request write, final DateRange range,
        final Span parentSpan
    ) {
        return suggest
            .write(new WriteSuggest.Request(write.getSeries(), range), parentSpan)
            .directTransform(Ingestion::fromWriteSuggest);
    }

    /**
     * Setup a range supplier that memoizes the result.
     */
    protected Supplier<DateRange> rangeSupplier(final Ingestion.Request request) {
        return new Supplier<DateRange>() {
            // memoized value.
            private DateRange calculated = null;

            @Override
            public DateRange get() {
                if (calculated != null) {
                    return calculated;
                }

                calculated = rangeFrom(request);
                return calculated;
            }
        };
    }

    protected DateRange rangeFrom(final Ingestion.Request request) {
        final Iterator<? extends Metric> it = request.all();

        if (!it.hasNext()) {
            throw new IllegalArgumentException("write batch must not be empty");
        }

        final Metric first = it.next();

        long start = first.getTimestamp();
        long end = first.getTimestamp();

        while (it.hasNext()) {
            final Metric d = it.next();
            start = Math.min(d.getTimestamp(), start);
            end = Math.max(d.getTimestamp(), end);
        }

        return new DateRange(start, end);
    }
}
