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

package com.spotify.heroic.metadata;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.SelectedGroup;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@ToString(of = {"backends"})
public class MetadataBackendGroup implements MetadataBackend {
    private final SelectedGroup<MetadataBackend> backends;
    private final AsyncFramework async;
    private final LocalMetadataManagerReporter reporter;

    @Override
    public AsyncFuture<Void> configure() {
        final List<AsyncFuture<Void>> callbacks = new ArrayList<>();
        run(backend -> backend.configure());
        return async.collectAndDiscard(callbacks);
    }

    @Override
    public AsyncFuture<FindTags> findTags(final RangeFilter filter) {
        final List<AsyncFuture<FindTags>> callbacks = run(b -> b.findTags(filter));
        return async.collect(callbacks, FindTags.reduce()).onDone(reporter.reportFindTags());
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final RangeFilter filter) {
        final List<AsyncFuture<CountSeries>> callbacks = run(b -> b.countSeries(filter));
        return async.collect(callbacks, CountSeries.reduce()).onDone(reporter.reportCountSeries());
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final RangeFilter filter) {
        final List<AsyncFuture<FindSeries>> callbacks = run(v -> v.findSeries(filter));
        return async
            .collect(callbacks, FindSeries.reduce(filter.getLimit()))
            .onDone(reporter.reportFindTimeSeries());
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final RangeFilter filter) {
        final List<AsyncFuture<DeleteSeries>> callbacks = run(b -> b.deleteSeries(filter));
        return async.collect(callbacks, DeleteSeries.reduce());
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final RangeFilter filter) {
        final List<AsyncFuture<FindKeys>> callbacks = run(b -> b.findKeys(filter));
        return async.collect(callbacks, FindKeys.reduce()).onDone(reporter.reportFindKeys());
    }

    @Override
    public AsyncFuture<Void> refresh() {
        final List<AsyncFuture<Void>> callbacks = run(b -> b.refresh());
        return async.collectAndDiscard(callbacks).onDone(reporter.reportRefresh());
    }

    @Override
    public AsyncFuture<WriteResult> write(final Series series, final DateRange range) {
        final List<AsyncFuture<WriteResult>> callbacks = run(b -> b.write(series, range));
        return async.collect(callbacks, WriteResult.merger());
    }

    @Override
    public AsyncObservable<List<Series>> entries(final RangeFilter filter) {
        final List<AsyncObservable<List<Series>>> observables = new ArrayList<>();

        for (final MetadataBackend b : backends) {
            observables.add(b.entries(filter));
        }

        return AsyncObservable.chain(observables);
    }

    @Override
    public boolean isReady() {
        boolean ready = true;

        for (final MetadataBackend backend : backends) {
            ready = ready && backend.isReady();
        }

        return ready;
    }

    @Override
    public Groups getGroups() {
        return backends.groups();
    }

    @Override
    public boolean isEmpty() {
        return backends.isEmpty();
    }

    @Override
    public int size() {
        return backends.size();
    }

    @Override
    public Statistics getStatistics() {
        Statistics s = Statistics.empty();

        for (final MetadataBackend b : backends) {
            s = s.merge(b.getStatistics());
        }

        return s;
    }

    private <T> List<T> run(InternalOperation<T> op) {
        final ImmutableList.Builder<T> results = ImmutableList.builder();

        for (final MetadataBackend b : backends) {
            try {
                results.add(op.run(b));
            } catch (final Exception e) {
                throw new RuntimeException("setting up backend operation failed", e);
            }
        }

        return results.build();
    }

    public static interface InternalOperation<T> {
        T run(MetadataBackend backend) throws Exception;
    }
}
