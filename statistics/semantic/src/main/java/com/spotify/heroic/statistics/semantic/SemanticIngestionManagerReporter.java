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
import com.codahale.metrics.Meter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.ToString;

@ToString(of = {})
public class SemanticIngestionManagerReporter implements IngestionManagerReporter {
    private static final String COMPONENT = "ingestion-manager";

    private final FutureReporter metadataWrite;

    private final Counter concurrentWritesCounter;
    private final Meter droppedByFilter;

    public SemanticIngestionManagerReporter(SemanticMetricRegistry registry) {
        final MetricId id = MetricId.build().tagged("component", COMPONENT);
        this.metadataWrite = new SemanticFutureReporter(registry,
            id.tagged("what", "metadata-write", "unit", Units.FAILURE));
        this.concurrentWritesCounter =
            registry.counter(id.tagged("what", "concurrent-writes", "unit", Units.WRITE));
        this.droppedByFilter =
            registry.meter(id.tagged("what", "dropped-by-filter", "unit", Units.DROP));
    }

    @Override
    public Context reportMetadataWrite() {
        return metadataWrite.setup();
    }

    @Override
    public void reportDroppedByFilter() {
        droppedByFilter.mark();
    }

    @Override
    public void incrementConcurrentWrites() {
        concurrentWritesCounter.inc();
    }

    @Override
    public void decrementConcurrentWrites() {
        concurrentWritesCounter.dec();
    }
}
