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

package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolProvider;
import com.spotify.heroic.statistics.ThreadPoolReporter;

public class NoopLocalMetadataBackendReporter implements LocalMetadataBackendReporter {
    private NoopLocalMetadataBackendReporter() {
    }

    @Override
    public Context reportRefresh() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindTags() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindTagKeys() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindTimeSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportCountSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindKeys() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportWrite() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public void reportWriteCacheHit() {
    }

    @Override
    public void reportWriteCacheMiss() {
    }

    @Override
    public void reportWriteSuccess(long n) {
    }

    @Override
    public void reportWriteFailure(long n) {
    }

    @Override
    public void reportWriteBatchDuration(long millis) {
    }

    @Override
    public void newWriteThreadPool(ThreadPoolProvider provider) {
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return NoopThreadPoolReporter.get();
    }

    @Override
    public void reportWriteDroppedByRateLimit() {
    }

    private static final NoopLocalMetadataBackendReporter instance = new NoopLocalMetadataBackendReporter();

    public static NoopLocalMetadataBackendReporter get() {
        return instance;
    }
}
