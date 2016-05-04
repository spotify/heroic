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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@GeneratedScope
@ToString
public class GeneratedBackend extends AbstractMetricBackend {
    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(GeneratedBackend.class, "fetch");

    private static final List<BackendEntry> EMPTY_ENTRIES = new ArrayList<>();

    private final AsyncFramework async;
    private final Generator generator;
    private final Groups groups;

    @Inject
    public GeneratedBackend(
        final AsyncFramework async, final Generator generator, final Groups groups
    ) {
        super(async);
        this.async = async;
        this.generator = generator;
        this.groups = groups;
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
    public AsyncFuture<FetchData> fetch(
        MetricType source, Series series, DateRange range, FetchQuotaWatcher watcher,
        QueryOptions options
    ) {
        final Stopwatch w = Stopwatch.createStarted();

        if (source == MetricType.POINT) {
            final List<Point> data = generator.generatePoints(series, range, watcher);
            final QueryTrace trace = new QueryTrace(FETCH, w.elapsed(TimeUnit.NANOSECONDS));
            final ImmutableList<Long> times = ImmutableList.of(trace.getElapsed());
            final List<MetricCollection> groups = ImmutableList.of(MetricCollection.points(data));
            return async.resolved(new FetchData(series, times, groups, trace));
        }

        if (source == MetricType.EVENT) {
            final List<Event> data = generator.generateEvents(series, range, watcher);
            final QueryTrace trace = new QueryTrace(FETCH, w.elapsed(TimeUnit.NANOSECONDS));
            final ImmutableList<Long> times = ImmutableList.of(trace.getElapsed());
            final List<MetricCollection> groups = ImmutableList.of(MetricCollection.events(data));
            return async.resolved(new FetchData(series, times, groups, trace));
        }

        throw new IllegalArgumentException("unsupported source: " + source);
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
