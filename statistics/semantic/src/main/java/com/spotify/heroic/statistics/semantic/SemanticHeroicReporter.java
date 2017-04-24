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

package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.QueryReporter;
import com.spotify.heroic.statistics.AnalyticsReporter;
import com.spotify.heroic.statistics.ClusteredManager;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.statistics.SuggestBackendReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@ToString(of = {})
@RequiredArgsConstructor
public class SemanticHeroicReporter implements HeroicReporter {
    private final SemanticMetricRegistry registry;

    private final Set<ClusteredManager> clusteredManagers = new HashSet<>();

    @Override
    public ConsumerReporter newConsumer(String id) {
        return new SemanticConsumerReporter(registry, id);
    }

    @Override
    public IngestionManagerReporter newIngestionManager() {
        return new SemanticIngestionManagerReporter(registry);
    }

    @Override
    public AnalyticsReporter newAnalyticsReporter() {
        return new SemanticAnalyticsReporter(registry);
    }

    @Override
    public MetadataBackendReporter newMetadataBackend() {
        return new SemanticMetadataBackendReporter(registry);
    }

    @Override
    public SuggestBackendReporter newSuggestBackend() {
        return new SemanticSuggestBackendReporter(registry);
    }

    @Override
    public MetricBackendReporter newMetricBackend() {
        return new SemanticMetricBackendReporter(registry);
    }

    @Override
    public QueryReporter newQueryReporter() {
        return new SemanticQueryReporter(registry);
    }

    @Override
    public void registerShards(Set<Map<String, String>> knownShards) {
        final Set<ClusteredManager> clustered;

        synchronized (clusteredManagers) {
            clustered = new HashSet<>(clusteredManagers);
        }

        for (final ClusteredManager c : clustered) {
            c.registerShards(knownShards);
        }
    }
}
