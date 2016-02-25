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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolProvider;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.ToString;

@ToString(of = {"base"})
public class SemanticLocalMetadataBackendReporter implements LocalMetadataBackendReporter {
    private static final String COMPONENT = "metadata-backend";

    private final SemanticMetricRegistry registry;
    private final MetricId base;

    private final FutureReporter refresh;
    private final FutureReporter findTags;
    private final FutureReporter findTagKeys;
    private final FutureReporter findTimeSeries;
    private final FutureReporter countSeries;
    private final FutureReporter findKeys;
    private final FutureReporter write;

    private final Meter writeSuccess;
    private final Meter writeFailure;

    private final Meter writesDroppedByRateLimit;

    private final Meter writeCacheHit;
    private final Meter writeCacheMiss;

    private final Histogram writeBatchDuration;

    public SemanticLocalMetadataBackendReporter(SemanticMetricRegistry registry, MetricId base) {
        this.registry = registry;
        this.base = base.tagged("component", COMPONENT);

        refresh = new SemanticFutureReporter(registry,
            base.tagged("what", "refresh", "unit", Units.REFRESH));
        findTags = new SemanticFutureReporter(registry,
            base.tagged("what", "find-tags", "unit", Units.LOOKUP));
        findTagKeys = new SemanticFutureReporter(registry,
            base.tagged("what", "find-tag-keys", "unit", Units.LOOKUP));
        findTimeSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "find-time-series", "unit", Units.LOOKUP));
        countSeries = new SemanticFutureReporter(registry,
            base.tagged("what", "count-series", "unit", Units.LOOKUP));
        findKeys = new SemanticFutureReporter(registry,
            base.tagged("what", "find-keys", "unit", Units.LOOKUP));
        write =
            new SemanticFutureReporter(registry, base.tagged("what", "write", "unit", Units.WRITE));
        writeCacheHit = registry.meter(base.tagged("what", "write-cache-hit", "unit", Units.HIT));
        writeCacheMiss =
            registry.meter(base.tagged("what", "write-cache-miss", "unit", Units.MISS));
        writesDroppedByRateLimit =
            registry.meter(base.tagged("what", "writes-dropped-by-rate-limit", "unit", Units.DROP));
        writeSuccess = registry.meter(base.tagged("what", "write-success", "unit", Units.WRITE));
        writeFailure = registry.meter(base.tagged("what", "write-failure", "unit", Units.FAILURE));
        writeBatchDuration = registry.histogram(
            base.tagged("what", "write-bulk-duration", "unit", Units.MILLISECOND));
    }

    @Override
    public FutureReporter.Context reportRefresh() {
        return refresh.setup();
    }

    @Override
    public FutureReporter.Context reportFindTags() {
        return findTags.setup();
    }

    @Override
    public Context reportFindTagKeys() {
        return findTagKeys.setup();
    }

    @Override
    public FutureReporter.Context reportFindTimeSeries() {
        return findTimeSeries.setup();
    }

    @Override
    public FutureReporter.Context reportCountSeries() {
        return countSeries.setup();
    }

    @Override
    public FutureReporter.Context reportFindKeys() {
        return findKeys.setup();
    }

    @Override
    public FutureReporter.Context reportWrite() {
        return write.setup();
    }

    @Override
    public void reportWriteCacheHit() {
        writeCacheHit.mark();
    }

    @Override
    public void reportWriteCacheMiss() {
        writeCacheMiss.mark();
    }

    @Override
    public void reportWriteDroppedByRateLimit() {
        writesDroppedByRateLimit.mark();
    }

    @Override
    public void newWriteThreadPool(final ThreadPoolProvider provider) {
        registry.register(base.tagged("what", "write-thread-pool-size", "unit", Units.BYTE),
            new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return provider.getQueueSize();
                }
            });
    }

    @Override
    public void reportWriteSuccess(long n) {
        writeSuccess.mark(n);
    }

    @Override
    public void reportWriteFailure(long n) {
        writeFailure.mark(n);
    }

    @Override
    public void reportWriteBatchDuration(long millis) {
        writeBatchDuration.update(millis);
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return new SemanticThreadPoolReporter(registry, base);
    }
}
