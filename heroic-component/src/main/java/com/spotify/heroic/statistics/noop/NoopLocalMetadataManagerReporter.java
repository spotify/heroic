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
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;

public class NoopLocalMetadataManagerReporter implements LocalMetadataManagerReporter {
    private NoopLocalMetadataManagerReporter() {
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
    public Context reportTagKeySuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportKeySuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagValuesSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagValueSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public LocalMetadataBackendReporter newMetadataBackend(String id) {
        return NoopLocalMetadataBackendReporter.get();
    }

    private static final NoopLocalMetadataManagerReporter instance =
            new NoopLocalMetadataManagerReporter();

    public static NoopLocalMetadataManagerReporter get() {
        return instance;
    }
}
