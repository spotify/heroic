/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.statistics.opencensus;

import com.spotify.heroic.statistics.AnalyticsReporter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.statistics.MemcachedReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.statistics.QueryReporter;
import com.spotify.heroic.statistics.SuggestBackendReporter;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {})
@RequiredArgsConstructor
public class OpenCensusHeroicReporter implements HeroicReporter {

    @Override
    public ConsumerReporter newConsumer(final String id) {
        return null;
    }

    @Override
    public IngestionManagerReporter newIngestionManager() {
        return null;
    }

    @Override
    public AnalyticsReporter newAnalyticsReporter() {
        return null;
    }

    @Override
    public MetadataBackendReporter newMetadataBackend() {
        return null;
    }

    @Override
    public SuggestBackendReporter newSuggestBackend() {
        return null;
    }

    @Override
    public MetricBackendReporter newMetricBackend() {
        return null;
    }

    @Override
    public QueryReporter newQueryReporter() {
        return null;
    }

    @Override
    public MemcachedReporter newMemcachedReporter(final String consumerType) {
        return null;
    }

    @Override
    public void registerShards(final Set<Map<String, String>> knownShards) {

    }

    @Override
    public void registerCacheSize(final String id, final Supplier<Long> cacheSize) {

    }
}
