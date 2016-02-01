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

package com.spotify.heroic.analytics;

import com.google.inject.Inject;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricBackend;

import java.time.LocalDate;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

public class NullMetricAnalytics implements MetricAnalytics {
    private final AsyncFramework async;

    @Inject
    public NullMetricAnalytics(final AsyncFramework async) {
        this.async = async;
    }

    @Override
    public MetricBackend wrap(MetricBackend backend) {
        return backend;
    }

    @Override
    public AsyncObservable<SeriesHit> seriesHits(LocalDate date) {
        return AsyncObservable.empty();
    }

    @Override
    public AsyncFuture<Void> start() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<Void> stop() {
        return async.resolved();
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<Void> reportFetchSeries(LocalDate date, Series series) {
        return async.resolved();
    }
}
