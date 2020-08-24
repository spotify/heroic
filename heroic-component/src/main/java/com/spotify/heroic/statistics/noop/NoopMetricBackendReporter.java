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

import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.statistics.DataInMemoryReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;

public class NoopMetricBackendReporter implements MetricBackendReporter {

    public static final DataInMemoryReporter DATA_IN_MEMORY_REPORTER = new DataInMemoryReporter() {
        @Override
        public void reportRowsAccessed(final long n) {
        }

        @Override
        public void reportRowDensity(final double samplesPerSecond) {

        }

        @Override
        public void reportDataHasBeenRead(final long n) {

        }

        @Override
        public void reportDataNoLongerNeeded(final long n) {

        }

        @Override
        public void reportOperationEnded() {

        }
    };

    private NoopMetricBackendReporter() {
    }

    @Override
    public MetricBackend decorate(final MetricBackend backend) {
        return backend;
    }

    @Override
    public DataInMemoryReporter newDataInMemoryReporter() {
        return DATA_IN_MEMORY_REPORTER;
    }

    @Override
    public FutureReporter.Context reportFindSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public FutureReporter.Context reportQueryMetrics() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public void reportWritesDroppedBySize() {

    }

    private static final NoopMetricBackendReporter instance = new NoopMetricBackendReporter();

    public static NoopMetricBackendReporter get() {
        return instance;
    }
}
