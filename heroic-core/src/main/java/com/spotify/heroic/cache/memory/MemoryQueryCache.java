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

package com.spotify.heroic.cache.memory;

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.cache.CacheScope;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryResult;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@CacheScope
public class MemoryQueryCache implements QueryCache {
    private final ExpiringMap<Key, AsyncFuture<QueryResult>> cache;

    private final Object lock = new Object();

    @Inject
    public MemoryQueryCache() {
        this.cache = ExpiringMap.builder().variableExpiration().build();
    }

    @Override
    public AsyncFuture<QueryResult> load(
        MetricType source, Filter filter, DateRange range, AggregationInstance aggregationInstance,
        QueryOptions options, Supplier<AsyncFuture<QueryResult>> loader
    ) {
        /* can't be cached :( */
        if (aggregationInstance.cadence() <= 0) {
            return loader.get();
        }

        final Key k = new Key(source, filter, range, aggregationInstance, options);

        final AsyncFuture<QueryResult> result = cache.get(k);

        if (result != null) {
            return result;
        }

        synchronized (lock) {
            final AsyncFuture<QueryResult> candidate = cache.get(k);

            if (candidate != null) {
                return candidate;
            }

            final AsyncFuture<QueryResult> next = loader.get();
            cache.put(k, next, ExpirationPolicy.ACCESSED, aggregationInstance.cadence(),
                TimeUnit.MILLISECONDS);
            return next;
        }
    }

    @Data
    public static class Key {
        private final MetricType source;
        private final Filter filter;
        private final DateRange range;
        private final AggregationInstance aggregationInstance;
        private final QueryOptions options;
    }
}
