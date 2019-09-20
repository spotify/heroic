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

data class TagValuesSuggest(
    val errors: List<RequestError>,
    val suggestions: List<Suggestion>,
    val limited: Boolean
) {
    constructor(suggestions: List<Suggestion>, limited: Boolean):
        this(emptyList(), suggestions, limited)

    companion object {
        @JvmStatic fun reduce(limit: OptionalLimit, groupLimit: OptionalLimit):
            Collector<TagValuesSuggest, TagValuesSuggest> {
            return Collector { groups: Collection<TagValuesSuggest> ->
                val errors: MutableList<RequestError> = mutableListOf()
                val midflights: MutableMap<String, MidFlight> = hashMapOf()
                var limited1 = false

                groups.forEach { group ->
                    errors.addAll(group.errors)
                    group.suggestions.forEach {
                        val m = midflights.getOrPut(it.key, { MidFlight() })
                        m.values.addAll(it.values)
                        m.limited = m.limited || it.limited
                    }
                    limited1 = limited1 || group.limited
                }

                val suggestions1 = midflights.map {
                    val m = it.value
                    val sLimited = m.limited || groupLimit.isGreater(m.values.size.toLong())
                    Suggestion(it.key, groupLimit.limitSortedSet(m.values), sLimited)
                }.sorted()

                TagValuesSuggest(
                    errors,
                    limit.limitList(suggestions1),
                    limited1 || limit.isGreater(suggestions1.size.toLong())
                )
            }
        }

        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, TagValuesSuggest> {
            return Transform { e: Throwable ->
                TagValuesSuggest(listOf(ShardError.fromThrowable(shard, e)), listOf(), false)
            }
        }
    }

    data class Suggestion(
        val key: String,
        val values: SortedSet<String>,
        val limited: Boolean
    ): Comparable<Suggestion> {
        override fun compareTo(other: Suggestion): Int {
            val v = -values.size.compareTo(other.values.size)
            if (v !=0) return v

            val k = key.compareTo(other.key)
            if (k != 0) return k

            val left = values.iterator()
            val right = other.values.iterator()

            while (left.hasNext()) {
                if (!right.hasNext()) return -1
                val l = left.next()
                val r = right.next()

                val kv = l.compareTo(r)
                if (kv != 0) return kv
            }

            if (right.hasNext()) return 1

            return limited.compareTo(other.limited)
        }
    }

    private data class MidFlight(
        val values: SortedSet<String> = TreeSet(),
        var limited: Boolean = false
    )

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit,
        val groupLimit: OptionalLimit,
        val exclude: List<String>
    )
}