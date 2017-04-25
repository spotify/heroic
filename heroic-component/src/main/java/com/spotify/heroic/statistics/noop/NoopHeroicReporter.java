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

import com.spotify.heroic.statistics.QueryReporter;
import com.spotify.heroic.statistics.AnalyticsReporter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.statistics.SuggestBackendReporter;

import java.util.Map;
import java.util.Set;

public class NoopHeroicReporter implements HeroicReporter {
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
    public MetadataBackendReporter newMetadataBackend() {
        return NoopMetadataBackendReporter.get();
    }

    @Override
    public SuggestBackendReporter newSuggestBackend() {
        return NoopSuggestBackendReporter.get();
    }

    @Override
    public MetricBackendReporter newMetricBackend() {
        return NoopMetricBackendReporter.get();
    }

    @Override
    public QueryReporter newQueryReporter() {
        return NoopQueryReporter.get();
    }

    @Override
    public void registerShards(Set<Map<String, String>> knownShards) {
    }

    private static final NoopHeroicReporter instance = new NoopHeroicReporter();

    public static NoopHeroicReporter get() {
        return instance;
    }
}
