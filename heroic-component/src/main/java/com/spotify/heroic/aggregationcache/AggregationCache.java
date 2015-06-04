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

import java.util.List;
import java.util.Map;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregationcache.model.CachePutResult;
import com.spotify.heroic.aggregationcache.model.CacheQueryResult;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

import eu.toolchain.async.AsyncFuture;

public interface AggregationCache {
    public boolean isConfigured();

    public AsyncFuture<CacheQueryResult> get(Filter filter, Map<String, String> group, Aggregation aggregation,
            DateRange range) throws CacheOperationException;

    public AsyncFuture<CachePutResult> put(Filter filter, Map<String, String> group, Aggregation aggregation,
            List<DataPoint> datapoints) throws CacheOperationException;
}
