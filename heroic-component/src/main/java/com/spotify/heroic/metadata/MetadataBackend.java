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

package com.spotify.heroic.metadata;

import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Initializing;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.WriteResult;

import java.util.List;

import eu.toolchain.async.AsyncFuture;

public interface MetadataBackend extends Grouped, Initializing {
    AsyncFuture<Void> configure();

    /**
     * Buffer a write for the specified series.
     *
     * @param id Id of series to write.
     * @param series Series to write.
     * @throws MetadataException If write could not be buffered.
     */
    AsyncFuture<WriteResult> write(Series series, DateRange range);

    AsyncFuture<Void> refresh();

    /**
     * Iterate <em>all</em> available metadata.
     *
     * This should perform pagination internally to avoid using too much memory.
     */
    default AsyncObservable<List<Series>> entries(RangeFilter filter) {
        return AsyncObservable.empty();
    }

    AsyncFuture<FindTags> findTags(RangeFilter filter);

    AsyncFuture<FindSeries> findSeries(RangeFilter filter);

    AsyncFuture<CountSeries> countSeries(RangeFilter filter);

    AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter);

    AsyncFuture<FindKeys> findKeys(RangeFilter filter);

    default Statistics getStatistics() {
        return Statistics.empty();
    }
}
