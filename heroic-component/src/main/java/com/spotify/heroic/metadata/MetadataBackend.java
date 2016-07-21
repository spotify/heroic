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
import com.spotify.heroic.common.Collected;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Initializing;
import com.spotify.heroic.common.Statistics;
import eu.toolchain.async.AsyncFuture;

public interface MetadataBackend extends Grouped, Initializing, Collected {
    AsyncFuture<Void> configure();

    /**
     * Buffer a write for the specified series.
     */
    AsyncFuture<WriteMetadata> write(WriteMetadata.Request request);

    /**
     * Iterate <em>all</em> available metadata.
     * <p>
     * This should perform pagination internally to avoid using too much memory.
     */
    default AsyncObservable<Entries> entries(Entries.Request request) {
        return AsyncObservable.empty();
    }

    AsyncFuture<FindTags> findTags(FindTags.Request request);

    AsyncFuture<FindSeries> findSeries(FindSeries.Request request);

    default AsyncObservable<FindSeriesStream> findSeriesStream(FindSeries.Request request) {
        return AsyncObservable.empty();
    }

    AsyncFuture<FindSeriesIds> findSeriesIds(FindSeriesIds.Request request);

    default AsyncObservable<FindSeriesIdsStream> findSeriesIdsStream(
        FindSeriesIds.Request request
    ) {
        return AsyncObservable.empty();
    }

    AsyncFuture<CountSeries> countSeries(CountSeries.Request request);

    AsyncFuture<DeleteSeries> deleteSeries(DeleteSeries.Request request);

    AsyncFuture<FindKeys> findKeys(FindKeys.Request request);

    default Statistics getStatistics() {
        return Statistics.empty();
    }
}
