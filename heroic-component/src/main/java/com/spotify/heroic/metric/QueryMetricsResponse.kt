/*
 * Copyright (c) 2019 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"): you may not use this file except in compliance
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

package com.spotify.heroic.metric

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.Statistics
import java.util.*

/**
 * Super important class that represents a response coming from e.g. BigTable.
 * The payload is in the `result` property and the other properties are metadata
 * pretty much.
 */
@JsonSerialize(using = QueryMetricsResponseSerializer::class)
data class QueryMetricsResponse @JvmOverloads constructor(
        val queryId: UUID,  // event tracing uses this
        val range: DateRange,
        val result: List<ShardedResultGroup>,
        val statistics: Statistics = Statistics.empty(),
        val errors: List<RequestError>,
        val trace: QueryTrace,
        val limits: ResultLimits,
        val preAggregationSampleSize: Optional<Long>,
        val cache: Optional<CacheInfo>
) {
    fun summarize(): Summary =
            Summary(range, ShardedResultGroup.summarize(result), statistics, errors, trace, limits,
                    preAggregationSampleSize, cache)

    // Only include data suitable to log to query log
    data class Summary(
            val range: DateRange,
            val result: ShardedResultGroup.MultiSummary,
            val statistics: Statistics,
            val errors: List<RequestError>,
            val trace: QueryTrace,
            val limits: ResultLimits,
            val preAggregationSampleSize: Optional<Long>,
            val cache: Optional<CacheInfo>
    )
}
