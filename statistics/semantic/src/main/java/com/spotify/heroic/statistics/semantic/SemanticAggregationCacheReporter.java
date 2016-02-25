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

import com.codahale.metrics.Histogram;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {})
@RequiredArgsConstructor
public class SemanticAggregationCacheReporter implements AggregationCacheReporter {
    private static final String COMPONENT = "aggregation-cache";

    private final FutureReporter get;
    private final FutureReporter put;
    private final Histogram getMiss;

    private final SemanticMetricRegistry registry;

    public SemanticAggregationCacheReporter(SemanticMetricRegistry registry) {
        this.registry = registry;

        final MetricId id = MetricId.build().tagged("component", COMPONENT);

        this.get =
            new SemanticFutureReporter(registry, id.tagged("what", "get", "unit", Units.READ));
        this.put =
            new SemanticFutureReporter(registry, id.tagged("what", "put", "unit", Units.WRITE));
        getMiss = registry.histogram(id.tagged("what", "get-miss", "unit", Units.MISS));
    }

    @Override
    public FutureReporter.Context reportGet() {
        return get.setup();
    }

    @Override
    public FutureReporter.Context reportPut() {
        return put.setup();
    }

    @Override
    public void reportGetMiss(int size) {
        getMiss.update(size);
    }

    @Override
    public AggregationCacheBackendReporter newAggregationCacheBackend() {
        return new SemanticAggregationCacheBackendReporter(registry);
    }
}
