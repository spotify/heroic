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

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.metric.Point;

import eu.toolchain.async.AsyncFuture;

/**
 * Is used to query for pre-aggregated cached time series.
 *
 * @author udoprog
 */
public interface AggregationCacheBackend extends LifeCycle {
    /**
     * Get an entry from the cache.
     *
     * @param key The entry key to get.
     * @return A callback that will be executed when the entry is available with the datapoints contained in the entry.
     *         This array can contain null values to indicate that entries are missing.
     * @throws CacheOperationException
     */
    public AsyncFuture<CacheBackendGetResult> get(CacheBackendKey key, DateRange range) throws CacheOperationException;

    /**
     * Put a new entry into the aggregation cache.
     *
     *
     *
     * @param key
     * @param datapoints An array of datapoints, <code>null</code> entries should be ignored.
     * @return A callback that will be executed as soon as any underlying request has been satisfied.
     * @throws CacheOperationException An early throw exception, if the backend is unable to prepare the request.
     */
    public AsyncFuture<CacheBackendPutResult> put(CacheBackendKey key, List<Point> datapoints)
            throws CacheOperationException;
}
