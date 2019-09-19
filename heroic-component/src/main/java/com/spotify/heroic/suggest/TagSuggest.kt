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

data class TagSuggest(
    val errors: List<RequestError> = emptyList(),
    val suggestions: List<Suggestion> = emptyList()
) {
    constructor(suggestions: List<Suggestion>): this(emptyList(), suggestions)

    companion object {
        @JvmStatic fun reduce(limit: OptionalLimit): Collector<TagSuggest, TagSuggest> {
            return Collector { groups: Collection<TagSuggest> ->
                val errors1: MutableList<RequestError> = mutableListOf()
                val suggestions1: MutableMap<Pair<String, String>, Float> = hashMapOf()

                groups.forEach { group ->
                    errors1.addAll(group.errors)
                    group.suggestions.forEach { suggestion ->
                        val key = suggestion.key to suggestion.value
                        val old = suggestions1.put(key, suggestion.score)

                        // prefer higher score if available
                        if (old != null && suggestion.score < old) {
                            suggestions1[key] = old
                        }
                    }
                }

                val values = suggestions1
                    .map { Suggestion(it.value, it.key.first, it.key.second) }
                    .sorted()
                TagSuggest(errors1, limit.limitList(values))
            }
        }

        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, TagSuggest> {
            return Transform { e: Throwable ->
                TagSuggest(errors = listOf(ShardError.fromThrowable(shard, e)))
            }
        }

    }

    data class Suggestion(
        val score: Float,
        val key: String,
        val value: String
    ): Comparable<Suggestion> {
        override fun compareTo(other: Suggestion): Int {
            val s = other.score.compareTo(score)
            if (s != 0) return s

            val k = key.compareTo(other.key)
            if (k != 0) return k

            return value.compareTo(other.value)
        }
    }

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit,
        val options: MatchOptions,
        val key: Optional<String>,
        val value: Optional<String>
    )
}
