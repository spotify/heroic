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

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class CoreIngestionGroup implements IngestionGroup {
    private final AsyncFramework async;
    private final Supplier<Filter> filter;
    private final Semaphore writePermits;
    private final IngestionManagerReporter reporter;
    private final LongAdder ingested;

    private final Optional<MetricBackend> metric;
    private final Optional<MetadataBackend> metadata;
    private final Optional<SuggestBackend> suggest;

    @Override
    public Groups getGroups() {
        return Groups.combine(metric.map(Grouped::getGroups).orElseGet(Groups::empty),
            metadata.map(Grouped::getGroups).orElseGet(Groups::empty),
            suggest.map(Grouped::getGroups).orElseGet(Groups::empty));
    }

    @Override
    public int size() {
        return metric.map(Grouped::size).orElse(0) + metadata.map(Grouped::size).orElse(0) +
            suggest.map(Grouped::size).orElse(0);
    }

    @Override
    public boolean isEmpty() {
        return metric.map(Grouped::isEmpty).orElse(true) &&
            metadata.map(Grouped::isEmpty).orElse(true) &&
            suggest.map(Grouped::isEmpty).orElse(true);
    }

    @Override
    public AsyncFuture<WriteResult> write(final WriteMetric write) {
        if (write.isEmpty()) {
            return async.resolved(WriteResult.of());
        }

        ingested.increment();
        return syncWrite(write);
    }

    protected AsyncFuture<WriteResult> syncWrite(final WriteMetric write) {
        if (!filter.get().apply(write.getSeries())) {
            // XXX: report dropped-by-filter
            return async.resolved(WriteResult.of());
        }

        try {
            writePermits.acquire();
        } catch (final InterruptedException e) {
            return async.failed(new Exception("Failed to acquire semaphore for bounded write", e));
        }

        reporter.incrementConcurrentWrites();

        return doWrite(write).onFinished(() -> {
            writePermits.release();
            reporter.decrementConcurrentWrites();
        });
    }

    protected AsyncFuture<WriteResult> doWrite(final WriteMetric write) {
        final List<AsyncFuture<WriteResult>> futures = new ArrayList<>();

        final Supplier<DateRange> range = rangeSupplier(write);

        metric.map(m -> doMetricWrite(m, write)).ifPresent(futures::add);
        metadata.map(m -> doMetadataWrite(m, write, range.get())).ifPresent(futures::add);
        suggest.map(s -> doSuggestWrite(s, write, range.get())).ifPresent(futures::add);

        return async.collect(futures, WriteResult.merger());
    }

    /**
     * Setup a range supplier that memoizes the result.
     */
    protected Supplier<DateRange> rangeSupplier(final WriteMetric write) {
        return new Supplier<DateRange>() {
            // memoized value.
            private DateRange calculated = null;

            @Override
            public DateRange get() {
                if (calculated != null) {
                    return calculated;
                }

                calculated = rangeFrom(write);
                return calculated;
            }
        };
    }

    protected AsyncFuture<WriteResult> doMetricWrite(
        final MetricBackend metric, final WriteMetric write
    ) {
        try {
            return metric.write(write);
        } catch (final Exception e) {
            return async.failed(e);
        }
    }

    protected AsyncFuture<WriteResult> doMetadataWrite(
        final MetadataBackend metadata, final WriteMetric write, final DateRange range
    ) {
        try {
            return metadata.write(write.getSeries(), range);
        } catch (final Exception e) {
            return async.failed(e);
        }
    }

    protected AsyncFuture<WriteResult> doSuggestWrite(
        final SuggestBackend suggest, final WriteMetric write, final DateRange range
    ) {
        try {
            return suggest.write(write.getSeries(), range);
        } catch (final Exception e) {
            return async.failed(e);
        }
    }

    protected DateRange rangeFrom(final WriteMetric write) {
        final Iterator<Metric> iterator = write.all().iterator();

        if (!iterator.hasNext()) {
            throw new IllegalArgumentException("write batch must not be empty");
        }

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
