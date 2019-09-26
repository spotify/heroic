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

import com.google.common.collect.ImmutableSortedSet
import com.spotify.heroic.common.Series
import java.util.*

/**
 * Gathers keys, tags, and resources and partitions them.
 *
 * Tags are partitioned by their key, their values are sorted and unique.
 * Resources are partitioned by their key, their values are sorted and unique.
 *
 * This is accomplished through a {@link Builder}, which permits building the data structure
 * incrementally.
 */
data class SeriesValues(
    val keys: SortedSet<String> = ImmutableSortedSet.of(),
    val tags: Map<String, SortedSet<String>> = emptyMap(),
    val resource: Map<String, SortedSet<String>> = emptyMap()
) {
    companion object {
        private val COMPARATOR: Comparator<String?> = Comparator { a: String?, b: String? ->
            if (a == null) {
                return@Comparator if (b == null) 0 else -1
            }
            if (b == null) return@Comparator 1

            a.compareTo(b)
        }

        /**
         * Construct a SeriesValues instance from an iterator of series.
         *
         * This is a convenience method over using {@link Builder} directly.
         *
         * @param series Series to build for
         * @return a new SeriesValues instance
         */
        @JvmStatic
        fun fromSeries(series: Iterator<Series>): SeriesValues {
            val keys: SortedSet<String> = TreeSet()
            val tags = mutableMapOf<String, SortedSet<String>>()
            val resource = mutableMapOf<String, SortedSet<String>>()

            series.forEachRemaining { s ->
                keys.add(s.key)
                s.tags.forEach {(key, value) ->
                    val values = tags.computeIfAbsent(key) { TreeSet(COMPARATOR) }
                    values.add(value)
                }

                s.resource.forEach {(key, value) ->
                    val values = resource.computeIfAbsent(key) { TreeSet(COMPARATOR) }
                    values.add(value)
                }
            }

            return SeriesValues(keys, tags.toMap(), resource.toMap())
        }
    }
}
