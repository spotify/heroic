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

import com.codahale.metrics.Counter;
import com.spotify.heroic.statistics.AnalyticsReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.ToString;

@ToString(of = {})
public class SemanticAnalyticsReporter implements AnalyticsReporter {
    private static final String COMPONENT = "analytics";

    private final Counter droppedFetchSeries;
    private final Counter failedFetchSeries;

    public SemanticAnalyticsReporter(SemanticMetricRegistry registry) {
        final MetricId id = MetricId.build().tagged("component", COMPONENT);

        this.droppedFetchSeries =
            registry.counter(id.tagged("what", "dropped-fetch-series", "unit", Units.DROP));
        this.failedFetchSeries =
            registry.counter(id.tagged("what", "failed-fetch-series", "unit", Units.FAILURE));
    }

    @Override
    public void reportDroppedFetchSeries() {
        droppedFetchSeries.inc();
    }

    @Override
    public void reportFailedFetchSeries() {
        failedFetchSeries.inc();
    }
}
