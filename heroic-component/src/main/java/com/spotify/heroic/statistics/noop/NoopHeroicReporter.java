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

import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.AnalyticsReporter;
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.HttpClientManagerReporter;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendGroupReporter;

import java.util.Map;
import java.util.Set;

public class NoopHeroicReporter implements HeroicReporter {
    @Override
    public LocalMetricManagerReporter newLocalMetricBackendManager() {
        return NoopLocalMetricManagerReporter.get();
    }

    @Override
    public ClusteredMetricManagerReporter newClusteredMetricBackendManager() {
        return NoopClusteredMetricManagerReporter.get();
    }

    @Override
    public LocalMetadataManagerReporter newLocalMetadataBackendManager() {
        return NoopLocalMetadataManagerReporter.get();
    }

    @Override
    public MetricBackendGroupReporter newMetricBackendsReporter() {
        return NoopMetricBackendsReporter.get();
    }

    @Override
    public ClusteredMetadataManagerReporter newClusteredMetadataBackendManager() {
        return NoopClusteredMetadataManagerReporter.get();
    }

    @Override
    public AggregationCacheReporter newAggregationCache() {
        return NoopAggregationCacheReporter.get();
    }

    @Override
    public HttpClientManagerReporter newHttpClientManager() {
        return NoopHttpClientManagerReporter.get();
    }

    @Override
    public ConsumerReporter newConsumer(String id) {
        return NoopConsumerReporter.get();
    }

    @Override
    public IngestionManagerReporter newIngestionManager() {
        return NoopIngestionManagerReporter.get();
    }

    @Override
    public AnalyticsReporter newAnalyticsReporter() {
        return NoopAnalyticsReporter.get();
    }

    @Override
    public void registerShards(Set<Map<String, String>> knownShards) {
    }

    private static final NoopHeroicReporter instance = new NoopHeroicReporter();

    public static NoopHeroicReporter get() {
        return instance;
    }
}
