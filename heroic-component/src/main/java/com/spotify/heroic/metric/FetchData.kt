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

import com.spotify.heroic.QueryOptions
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.Series
import eu.toolchain.async.Collector

data class FetchData(
    val result: Result,
    val times: List<Long>,
    val groups: List<MetricCollection>
) {
    companion object {
        @JvmStatic fun collectResult(what: QueryTrace.Identifier): Collector<Result, Result> {
            val w = Tracing.DEFAULT.watch(what)
            return Collector { result: Collection<Result> ->
                val traces = mutableListOf<QueryTrace>()
                val errors = mutableListOf<RequestError>()
                result.forEach {
                    traces.add(it.trace)
                    errors.addAll(it.errors)
                }
                Result(w.end(traces.toList()), errors.toList())
            }
        }
    }

    data class Request(
        val type: MetricType,
        val series: Series,
        val range: DateRange,
        val options: QueryOptions
    )

    data class Result @JvmOverloads constructor(
        val trace: QueryTrace,
        val errors: List<RequestError> = listOf()
    ) {
        constructor(trace: QueryTrace, error: RequestError): this(trace, listOf(error))
    }
}
