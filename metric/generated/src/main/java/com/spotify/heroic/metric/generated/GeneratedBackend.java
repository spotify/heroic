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

package com.spotify.heroic.metric.generated;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypedGroup;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString
public class GeneratedBackend extends AbstractMetricBackend implements LifeCycle {
    private static final List<BackendEntry> EMPTY_ENTRIES = new ArrayList<>();

    private final AsyncFramework async;
    private final Generator generator;
    private final Groups groups;

    @Inject
    public GeneratedBackend(final AsyncFramework async, final Generator generator, final Groups groups) {
        super(async);
        this.async = async;
        this.generator = generator;
        this.groups = groups;
    }

    @Override
    public AsyncFuture<Void> start() {
        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> stop() {
        return async.resolved(null);
    }

    @Override
    public Groups getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<WriteResult> write(WriteMetric write) {
        return async.resolved(WriteResult.EMPTY);
    }

    @Override
    public AsyncFuture<WriteResult> write(Collection<WriteMetric> writes) {
        return async.resolved(WriteResult.EMPTY);
    }

    @Override
    public AsyncFuture<FetchData> fetch(MetricType source, Series series, DateRange range,
            FetchQuotaWatcher watcher) {
        final long start = System.nanoTime();

        if (source == MetricType.POINT) {
            final List<Metric> data = generator.generate(series, range, watcher);
            final long diff = System.nanoTime() - start;
            final ImmutableList<Long> times = ImmutableList.of(diff);
            final List<MetricTypedGroup> groups = ImmutableList.of(new MetricTypedGroup(MetricType.POINT, data));
            return async.resolved(new FetchData(series, times, groups));
        }

        if (source == MetricType.EVENT) {
            final List<Metric> data = generator.generateEvents(series, range, watcher);
            final long diff = System.nanoTime() - start;
            final ImmutableList<Long> times = ImmutableList.of(diff);
            final List<MetricTypedGroup> groups = ImmutableList.of(new MetricTypedGroup(MetricType.POINT, data));
            return async.resolved(new FetchData(series, times, groups));
        }

        throw new IllegalArgumentException("unsupported source: " + source);
    }

    @Override
    public AsyncFuture<List<BackendKey>> keys(BackendKey start, BackendKey end, int limit) {
        return async.resolved((List<BackendKey>) new ArrayList<BackendKey>());
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return EMPTY_ENTRIES;
    }
}