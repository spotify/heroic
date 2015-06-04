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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.google.common.collect.Iterables;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.CountSeries;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.MetadataEntry;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.utils.SelectedGroup;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@RequiredArgsConstructor
@ToString(of = { "backends" })
public class MetadataBackendGroup implements MetadataBackend {
    private final SelectedGroup<MetadataBackend> backends;
    private final AsyncFramework async;
    private final LocalMetadataManagerReporter reporter;

    @Override
    public AsyncFuture<FindTags> findTags(final RangeFilter filter) {
        final List<AsyncFuture<FindTags>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, MetadataBackend backend) throws Exception {
                callbacks.add(backend.findTags(filter));
            }
        });

        return async.collect(callbacks, FindTags.reduce()).onAny(reporter.reportFindTags());
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final RangeFilter filter) {
        final List<AsyncFuture<CountSeries>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, MetadataBackend backend) throws Exception {
                callbacks.add(backend.countSeries(filter));
            }
        });

        return async.collect(callbacks, CountSeries.reduce()).onAny(reporter.reportCountSeries());
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final RangeFilter filter) {
        final List<AsyncFuture<FindSeries>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, MetadataBackend backend) throws Exception {
                callbacks.add(backend.findSeries(filter));
            }
        });

        return async.collect(callbacks, FindSeries.reduce(filter.getLimit())).onAny(reporter.reportFindTimeSeries());
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final RangeFilter filter) {
        final List<AsyncFuture<DeleteSeries>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, MetadataBackend backend) throws Exception {
                callbacks.add(backend.deleteSeries(filter));
            }
        });

        return async.collect(callbacks, DeleteSeries.reduce());
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final RangeFilter filter) {
        final List<AsyncFuture<FindKeys>> callbacks = new ArrayList<AsyncFuture<FindKeys>>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, MetadataBackend backend) throws Exception {
                callbacks.add(backend.findKeys(filter));
            }
        });

        return async.collect(callbacks, FindKeys.reduce()).onAny(reporter.reportFindKeys());
    }

    @Override
    public AsyncFuture<Void> refresh() {
        final List<AsyncFuture<Void>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, MetadataBackend backend) throws Exception {
                callbacks.add(backend.refresh());
            }
        });

        return async.collectAndDiscard(callbacks).onAny(reporter.reportRefresh());
    }

    @Override
    public AsyncFuture<WriteResult> write(final Series series, final DateRange range) {
        final List<AsyncFuture<WriteResult>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, MetadataBackend backend) throws Exception {
                callbacks.add(backend.write(series, range));
            }
        });

        return async.collect(callbacks, WriteResult.merger());
    }

    @Override
    public Iterable<MetadataEntry> entries(final Filter filter, final DateRange range) {
        final List<Iterable<MetadataEntry>> entries = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, MetadataBackend backend) throws Exception {
                entries.add(backend.entries(filter, range));
            }
        });

        return Iterables.concat(entries);
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
    public Set<String> getGroups() {
        return backends.groups();
    }

    private void run(InternalOperation op) {
        for (final MetadataBackend b : backends) {
            try {
                op.run(backends.getDisabled(), b);
            } catch (final Exception e) {
                throw new RuntimeException("setting up backend operation failed", e);
            }
        }
    }

    public static interface InternalOperation {
        void run(int disabled, MetadataBackend backend) throws Exception;
    }
}