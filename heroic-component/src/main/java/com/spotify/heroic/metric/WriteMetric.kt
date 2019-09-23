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

package com.spotify.heroic.metric

import com.spotify.heroic.cluster.ClusterShard
import com.spotify.heroic.common.RequestTimer
import com.spotify.heroic.common.Series
import eu.toolchain.async.Collector
import eu.toolchain.async.Transform

data class WriteMetric(
    val errors: List<RequestError> = listOf(),
    val times: List<Long> = listOf()
) {
    constructor(time: Long): this(times = listOf(time))
    constructor(error: RequestError): this(errors = listOf(error))

    companion object {
        @JvmStatic fun shardError(c: ClusterShard): Transform<Throwable, WriteMetric> {
            return Transform { e: Throwable ->
                WriteMetric(listOf(ShardError.fromThrowable(c, e)))
            }
        }

        @JvmStatic fun reduce(): Collector<WriteMetric, WriteMetric> {
            return Collector { results: Collection<WriteMetric> ->
                val errors = mutableListOf<RequestError>()
                val times = mutableListOf<Long>()

                results.forEach {
                    errors.addAll(it.errors)
                    times.addAll(it.times)
                }

                WriteMetric(errors.toList(), times.toList())
            }
        }

        @JvmStatic fun timer() = RequestTimer(::WriteMetric)
    }

    data class Request(val series: Series, val data: MetricCollection)
}