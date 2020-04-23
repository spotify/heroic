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
import io.opencensus.trace.Span;

public interface MetadataBackend extends Grouped, Initializing, Collected {

    /**
     * Configure the metadata backend.
     *
     * This will assert that all required settings and mappings exists and are configured
     * correctly for the given backend.
     */
    AsyncFuture<Void> configure();

    /**
     * Buffer a write for the specified series.
     */
    AsyncFuture<WriteMetadata> write(WriteMetadata.Request request);
    default AsyncFuture<WriteMetadata> write(WriteMetadata.Request request, Span parentSpan) {
        // Ignore the parent span if the module does not specifically implement it.
        return write(request);
    }

    /**
     * Iterate <em>all</em> available metadata.
     * <p>
     * This should perform pagination internally to avoid using too much memory.
     */
    default AsyncObservable<Entries> entries(Entries.Request request) {
        return AsyncObservable.empty();
    }

    @Deprecated
    AsyncFuture<FindTags> findTags(FindTags.Request request);

    /**
     * List the full series that match the given filter.
     */
    AsyncFuture<FindSeries> findSeries(FindSeries.Request request);
    default AsyncObservable<FindSeriesStream> findSeriesStream(FindSeries.Request request) {
        return AsyncObservable.empty();
    }

    /**
     * List only the series id that match the given filter.
     */
    AsyncFuture<FindSeriesIds> findSeriesIds(FindSeries.Request request);
    default AsyncObservable<FindSeriesIdsStream> findSeriesIdsStream(FindSeries.Request request) {
        return AsyncObservable.empty();
    }

    /**
     * Count the number of series that match the given filter.
     */
    AsyncFuture<CountSeries> countSeries(CountSeries.Request request);

    /**
     * Delete the series from the backend.
     */
    AsyncFuture<DeleteSeries> deleteSeries(DeleteSeries.Request request);

    /**
     * List only the key part of the series that match the given filter.
     */
    AsyncFuture<FindKeys> findKeys(FindKeys.Request request);

    default Statistics getStatistics() {
        return Statistics.empty();
    }
}
