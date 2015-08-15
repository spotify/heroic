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

package com.spotify.heroic.aggregationcache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.statistics.AggregationCacheReporter;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@NoArgsConstructor
public class AggregationCacheImpl implements AggregationCache {
    @Inject
    private AggregationCacheBackend backend;

    @Inject
    private AggregationCacheReporter reporter;

    @RequiredArgsConstructor
    private static final class BackendCacheGetHandle implements Transform<CacheBackendGetResult, CacheQueryResult> {
        private final AggregationCacheReporter reporter;
        private final DateRange range;

        @Override
        public CacheQueryResult transform(CacheBackendGetResult result) throws Exception {
            final CacheBackendKey key = result.getKey();

            final long width = key.getAggregation().extent();

            final List<DateRange> misses = new ArrayList<DateRange>();

            final List<Point> cached = result.getDatapoints();

            if (width == 0 || cached.isEmpty()) {
                misses.add(range);
                reporter.reportGetMiss(misses.size());
                return new CacheQueryResult(key, range, cached, misses);
            }

            final long end = range.getEnd();

            long current = range.getStart();

            for (final Point d : cached) {
                if (current + width != d.getTimestamp() && current < d.getTimestamp())
                    misses.add(range.modify(current, d.getTimestamp()));

                current = d.getTimestamp();
            }

            if (current < end)
                misses.add(range.modify(current, end));

            reporter.reportGetMiss(misses.size());
            return new CacheQueryResult(key, range, cached, misses);
        }
    }

    @RequiredArgsConstructor
    private final class BackendCachePutHandle implements Transform<CacheBackendPutResult, CachePutResult> {
        @Override
        public CachePutResult transform(CacheBackendPutResult result) throws Exception {
            return new CachePutResult();
        }
    }

    @Override
    public boolean isConfigured() {
        return backend != null;
    }

    @Override
    public AsyncFuture<CacheQueryResult> get(Filter filter, Map<String, String> group, final Aggregation aggregation,
            DateRange range) throws CacheOperationException {
        if (!isConfigured())
            throw new CacheOperationException("Cache backend is not configured");

        final CacheBackendKey key = new CacheBackendKey(filter, group, aggregation);

        return backend.get(key, range).transform(new BackendCacheGetHandle(reporter, range));
    }

    @Override
    public AsyncFuture<CachePutResult> put(Filter filter, Map<String, String> group, Aggregation aggregation,
            List<Point> datapoints) throws CacheOperationException {
        final CacheBackendKey key = new CacheBackendKey(filter, group, aggregation);

        if (!isConfigured())
            throw new CacheOperationException("Cache backend is not configured");

        return backend.put(key, datapoints).transform(new BackendCachePutHandle()).onAny(reporter.reportPut());
    }
}
