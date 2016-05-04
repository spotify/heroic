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

package com.spotify.heroic.cache.noop;

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.cache.CacheScope;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryResult;
import eu.toolchain.async.AsyncFuture;

import javax.inject.Inject;
import java.util.function.Supplier;

@CacheScope
public class NoopQueryCache implements QueryCache {
    @Inject
    public NoopQueryCache() {
    }

    @Override
    public AsyncFuture<QueryResult> load(
        MetricType source, Filter filter, DateRange range, AggregationInstance aggregationInstance,
        QueryOptions options, Supplier<AsyncFuture<QueryResult>> loader
    ) {
        return loader.get();
    }
}
