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

import com.google.common.collect.Iterables
import com.spotify.heroic.cluster.ClusterShard
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.OptionalLimit
import com.spotify.heroic.filter.Filter
import com.spotify.heroic.metric.RequestError
import com.spotify.heroic.metric.ShardError
import eu.toolchain.async.Collector
import eu.toolchain.async.Transform
import java.util.*
import kotlin.collections.HashMap

data class TagKeyCount(
    val errors: List<RequestError>,
    val suggestions: List<Suggestion>,
    val limited: Boolean
) {
    constructor(suggestions: List<Suggestion>, limited: Boolean):
        this(emptyList(), suggestions, limited)

    companion object {
        @JvmStatic fun reduce(limit: OptionalLimit, exactLimit: OptionalLimit):
            Collector<TagKeyCount, TagKeyCount> {
            return Collector { groups: Collection<TagKeyCount> ->
                val errors1: MutableList<RequestError> = mutableListOf()
                val suggestions: HashMap<String, Suggestion> = hashMapOf()
                var limited = false

                groups.forEach { group ->
                    errors1.addAll(group.errors)
                    group.suggestions.forEach {
                        val replaced = suggestions.put(it.key, it)

                        if (replaced != null) {
                            val exactValues =
                                if (it.exactValues.isPresent && replaced.exactValues.isPresent) {
                                    Optional.of(
                                        Iterables.concat(
                                            it.exactValues.get(),
                                            replaced.exactValues.get()
                                        ).toSet()
                                    ).filter { set -> !exactLimit.isGreater(set.size.toLong()) }
                                } else {
                                    Optional.empty()
                                }
                            suggestions[it.key] =
                                Suggestion(it.key, it.count + replaced.count, exactValues)
                        }
                    }

                    limited = limited || group.limited
                }

                val list = suggestions.values.sorted()

                TagKeyCount(
                    errors1,
                    limit.limitList(list),
                    limited || limit.isGreater(list.size.toLong())
                )
            }
        }

        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, TagKeyCount> {
            return Transform { e: Throwable ->
                TagKeyCount(listOf(ShardError.fromThrowable(shard, e)), listOf(), false)
            }
        }
    }

    data class Suggestion(
        val key: String,
        val count: Long,
        val exactValues: Optional<Set<String>>
    ): Comparable<Suggestion> {
        override fun compareTo(other: Suggestion): Int {
            val k = key.compareTo(other.key)
            if (k != 0) return k

            return count().compareTo(other.count())
        }

        fun count(): Long = exactValues.map { it.size.toLong() }.orElse(count)
    }

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit,
        val exactLimit: OptionalLimit
    )
}