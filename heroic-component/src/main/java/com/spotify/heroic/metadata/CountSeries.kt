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

import com.spotify.heroic.cluster.ClusterShard
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.OptionalLimit
import com.spotify.heroic.filter.Filter
import com.spotify.heroic.metric.RequestError
import com.spotify.heroic.metric.ShardError
import eu.toolchain.async.Collector
import eu.toolchain.async.Transform

data class CountSeries(
    val errors: List<RequestError> = listOf(),
    val count: Long = 0,
    val limited: Boolean = false
) {
    constructor(count: Long, limited: Boolean):
        this(errors = listOf(), count = count, limited = limited)

    companion object {
        @JvmStatic fun reduce(): Collector<CountSeries, CountSeries> {
            return Collector { results: Collection<CountSeries> ->
                val errors = mutableListOf<RequestError>()
                var count = 0L
                var limited = false

                results.forEach {
                    errors.addAll(it.errors)
                    count += it.count
                    limited = limited || it.limited
                }

                CountSeries(errors, count, limited)
            }
        }

        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, CountSeries> {
            return Transform { e: Throwable ->
                CountSeries(errors = listOf(ShardError.fromThrowable(shard, e)))
            }
        }
    }

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit
    )
}