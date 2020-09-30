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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.common.hash.Hashing
import com.spotify.heroic.common.Histogram
import com.spotify.heroic.common.Series
import java.util.*

/**
 * Cornerstone result class that encapsulates a result from ? TODO.
 * The series property identifies the Series' returned and the metrics property
 * contains the actual numerical payload.
 */
data class ShardedResultGroup(
        val shard: Map<String, String>,
        // key-value pairs that act as a lookup key, identifying this result group
        val key: Map<String, String>,
        val series: Set<Series>,
        val metrics: MetricCollection,
        val cadence: Long
) {
    @JsonIgnore
    fun isEmpty() = metrics.isEmpty

    fun hashGroup(): Int {
        val hasher = HASH_FUNCTION.newHasher()
        shard.forEach { (key, value) ->
            hasher.putInt(RECORD_SEPARATOR)
            hasher.putString(key, Charsets.UTF_8)
            hasher.putInt(RECORD_SEPARATOR)
            hasher.putString(value, Charsets.UTF_8)
        }

        hasher.putInt(RECORD_SEPARATOR)

        key.forEach { (key, value) ->
            hasher.putInt(RECORD_SEPARATOR)
            hasher.putString(key, Charsets.UTF_8)
            hasher.putInt(RECORD_SEPARATOR)
            hasher.putString(value, Charsets.UTF_8)
        }

        return hasher.hash().asInt()
    }

    companion object {
        // record separator is needed to avoid conflicts
        private const val RECORD_SEPARATOR = 0
        private val HASH_FUNCTION = Hashing.murmur3_32()

        fun summarize(resultGroups: List<ShardedResultGroup>): MultiSummary {
            val shardSummary = mutableSetOf<Map<String, String>>()
            val keySize = Histogram.Builder()
            val seriesSummarizer = SeriesSetsSummarizer()
            val dataSize = Histogram.Builder()
            var cadence = Optional.empty<Long>()

            resultGroups.forEach {
                shardSummary.add(it.shard)
                keySize.add(it.key.size.toLong())
                seriesSummarizer.add(it.series)
                dataSize.add(it.metrics.size().toLong())
                cadence = Optional.of(it.cadence)
            }

            return MultiSummary(shardSummary.toSet(), keySize.build(),
                    seriesSummarizer.end(), dataSize.build(), cadence)
        }
    }

    data class MultiSummary(
            val shards: Set<Map<String, String>>,
            val keySize: Histogram,
            val series: SeriesSetsSummarizer.Summary,
            val dataSize: Histogram,
            val cadence: Optional<Long>
    )
}
