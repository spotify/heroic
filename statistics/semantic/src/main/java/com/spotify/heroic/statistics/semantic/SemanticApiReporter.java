/*
 * Copyright (c) 2017 Spotify AB.
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
import com.spotify.heroic.statistics.ApiReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString(of = {"base"})
public class SemanticApiReporter implements ApiReporter {
    private static final String COMPONENT = "api";

    private final FutureReporter query;
    private final Histogram smallQueryLatency;

    public SemanticApiReporter(final SemanticMetricRegistry registry) {
        final MetricId base = MetricId.build().tagged("component", COMPONENT);

        query =
            new SemanticFutureReporter(registry, base.tagged("what", "query", "unit", Units.QUERY));
        smallQueryLatency = registry.histogram(
            base.tagged("what", "small-query-latency", "unit", Units.MILLISECOND));
    }

    @Override
    public FutureReporter.Context reportQuery() {
        return query.setup();
    }

    @Override
    public void reportSmallQueryLatency(final long duration) {
        smallQueryLatency.update(duration);
    }
}
