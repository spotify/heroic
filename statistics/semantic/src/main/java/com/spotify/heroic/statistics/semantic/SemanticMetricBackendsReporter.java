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

import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.MetricBackendGroupReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {})
@RequiredArgsConstructor
public class SemanticMetricBackendsReporter implements MetricBackendGroupReporter {
    private static final String COMPONENT = "metric-backends";

    private final FutureReporter query;
    private final FutureReporter write;
    private final FutureReporter writeBatch;
    private final FutureReporter queryMetrics;
    private final FutureReporter findSeries;

    public SemanticMetricBackendsReporter(SemanticMetricRegistry registry) {
        final MetricId id = MetricId.build().tagged("component", COMPONENT);

        this.query =
            new SemanticFutureReporter(registry, id.tagged("what", "query", "unit", Units.READ));
        this.write =
            new SemanticFutureReporter(registry, id.tagged("what", "write", "unit", Units.WRITE));
        this.writeBatch = new SemanticFutureReporter(registry,
            id.tagged("what", "write-batch", "unit", Units.WRITE));
        this.queryMetrics = new SemanticFutureReporter(registry,
            id.tagged("what", "query-metrics", "unit", Units.READ));
        this.findSeries = new SemanticFutureReporter(registry,
            id.tagged("what", "find-series", "unit", Units.LOOKUP));
    }

    @Override
    public FutureReporter.Context reportQuery() {
        return query.setup();
    }

    @Override
    public FutureReporter.Context reportWrite() {
        return write.setup();
    }

    @Override
    public FutureReporter.Context reportWriteBatch() {
        return writeBatch.setup();
    }

    @Override
    public FutureReporter.Context reportQueryMetrics() {
        return queryMetrics.setup();
    }

    @Override
    public FutureReporter.Context reportFindSeries() {
        return findSeries.setup();
    }
}
