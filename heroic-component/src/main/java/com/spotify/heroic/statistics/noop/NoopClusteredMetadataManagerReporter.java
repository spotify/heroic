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

import java.util.Map;
import java.util.Set;

import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;

public class NoopClusteredMetadataManagerReporter implements ClusteredMetadataManagerReporter {
    private NoopClusteredMetadataManagerReporter() {
    }

    @Override
    public Context reportFindTags() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindTagsShard(Map<String, String> shard) {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindKeys() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindKeysShard(Map<String, String> shard) {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindSeriesShard(Map<String, String> shard) {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportDeleteSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportWrite() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportSearch() {
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
    public Context reportCount() {
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
    public void registerShards(Set<Map<String, String>> knownShards) {
    }

    private static final NoopClusteredMetadataManagerReporter instance =
            new NoopClusteredMetadataManagerReporter();

    public static NoopClusteredMetadataManagerReporter get() {
        return instance;
    }
}
