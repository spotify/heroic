/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.metadata

import com.fasterxml.jackson.annotation.JsonIgnore
import com.spotify.heroic.cluster.ClusterShard
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.OptionalLimit
import com.spotify.heroic.common.Series
import com.spotify.heroic.filter.Filter
import com.spotify.heroic.metric.RequestError
import com.spotify.heroic.metric.ShardError
import eu.toolchain.async.Collector
import eu.toolchain.async.Transform

data class FindSeries(
    val errors: List<RequestError> = listOf(),
    val series: Set<Series> = setOf(),
    val limited: Boolean = false
) {
    constructor(series: Set<Series>, limited: Boolean): this(listOf(), series, limited)

    companion object {
        @JvmStatic fun reduce(limit: OptionalLimit): Collector<FindSeries, FindSeries> {
            return Collector { results: Collection<FindSeries> ->
                val errors = mutableListOf<RequestError>()
                val series = mutableSetOf<Series>()
                var limited = false

                results.forEach {
                    errors.addAll(it.errors)
                    series.addAll(it.series)
                    limited = limited || it.limited
                }

                FindSeries(
                    errors,
                    limit.limitSet(series),
                    limited || limit.isGreater(series.size.toLong())
                )
            }
        }

        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, FindSeries> {
            return Transform { e: Throwable ->
                FindSeries(errors = listOf(ShardError.fromThrowable(shard, e)))
            }
        }
    }

    @JsonIgnore
    fun isEmpty(): Boolean = series.isEmpty()

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit
    )
}