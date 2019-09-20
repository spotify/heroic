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

package com.spotify.heroic.suggest

import com.spotify.heroic.cluster.ClusterShard
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.OptionalLimit
import com.spotify.heroic.filter.Filter
import com.spotify.heroic.metric.RequestError
import com.spotify.heroic.metric.ShardError
import eu.toolchain.async.Collector
import eu.toolchain.async.Transform
import java.util.*

data class KeySuggest(val errors: List<RequestError>, val suggestions: List<Suggestion>) {
    constructor(suggestions: List<Suggestion>): this(listOf(), suggestions)

    companion object {
        @JvmStatic fun reduce(limit: OptionalLimit): Collector<KeySuggest, KeySuggest> {
            return Collector { results: Collection<KeySuggest> ->
                val errors1: MutableList<RequestError> = mutableListOf()
                val suggestions1: MutableMap<String, Suggestion> = hashMapOf()

                results.forEach { result: KeySuggest ->
                    errors1.addAll(result.errors)
                    result.suggestions.forEach {
                        val alt = suggestions1[it.key]
                        if (alt == null || alt.score < it.score) {
                            suggestions1[it.key] = it
                        }
                    }
                }

                val list = suggestions1.values.sorted()
                KeySuggest(errors1, limit.limitList(list))
            }
        }

        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, KeySuggest> {
            return Transform { e: Throwable ->
                KeySuggest(listOf(ShardError.fromThrowable(shard, e)), listOf())
            }
        }
    }

    data class Suggestion(val score: Float, val key: String): Comparable<Suggestion> {
        override fun compareTo(other: Suggestion): Int {
            val s = other.score.compareTo(score)
            if (s != 0) return s

            return key.compareTo(other.key)
        }
    }

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit,
        val options: MatchOptions,
        val key: Optional<String>
    )
}