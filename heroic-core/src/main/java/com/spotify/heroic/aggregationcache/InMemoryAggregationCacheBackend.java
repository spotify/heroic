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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.ToString;

import com.google.inject.Inject;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.Point;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

/**
 * A reference aggregation cache implementation to allow for easier testing of application logic.
 *
 * @author udoprog
 */
@ToString
public class InMemoryAggregationCacheBackend implements AggregationCacheBackend {
    private final Map<CacheBackendKey, Map<Long, Point>> cache = new HashMap<CacheBackendKey, Map<Long, Point>>();

    @Inject
    private AsyncFramework async;

    @Override
    public synchronized AsyncFuture<CacheBackendGetResult> get(CacheBackendKey key, DateRange range)
            throws CacheOperationException {
        Map<Long, Point> entry = cache.get(key);

        if (entry == null) {
            entry = new HashMap<Long, Point>();
            cache.put(key, entry);
        }

        final Aggregation aggregation = key.getAggregation();

        final long extent = aggregation.extent();

        if (extent == 0)
            throw new CacheOperationException("provided aggregation is not cacheable");

        final List<Point> datapoints = new ArrayList<Point>();

        if (extent == 0) {
            return async.resolved(new CacheBackendGetResult(key, datapoints));
        }

        final long start = range.getStart() - range.getStart() % extent;
        final long end = range.getEnd() - range.getEnd() % extent;

        for (long i = start; i < end; i += extent) {
            final Point d = entry.get(i);

            if (d == null)
                continue;

            datapoints.add(d);
        }

        return async.resolved(new CacheBackendGetResult(key, datapoints));
    }

    @Override
    public synchronized AsyncFuture<CacheBackendPutResult> put(CacheBackendKey key, List<Point> datapoints)
            throws CacheOperationException {
        Map<Long, Point> entry = cache.get(key);

        if (entry == null) {
            entry = new HashMap<Long, Point>();
            cache.put(key, entry);
        }

        final Aggregation aggregation = key.getAggregation();
        final long extent = aggregation.extent();

        if (extent == 0)
            return async.resolved(new CacheBackendPutResult());

        for (final Point d : datapoints) {
            final long timestamp = d.getTimestamp();
            final double value = d.getValue();

            if (Double.isNaN(value))
                continue;

            if (timestamp % extent != 0)
                continue;

            entry.put(timestamp, d);
        }

        return async.resolved(new CacheBackendPutResult());
    }

    @Override
    public AsyncFuture<Void> start() {
        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> stop() {
        return async.resolved(null);
    }

    @Override
    public boolean isReady() {
        return true;
    }
}