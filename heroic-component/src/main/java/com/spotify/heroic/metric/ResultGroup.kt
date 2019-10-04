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

import com.spotify.heroic.cluster.ClusterShard
import com.spotify.heroic.common.Histogram
import com.spotify.heroic.common.Series
import java.util.*

data class ResultGroup(
    // key-value pairs that act as a lookup key, identifying this result group
    val key: Map<String, String>,
    val series: Set<Series>,
    val group: MetricCollection,
    /**
     * The interval in milliseconds for which a sample can be expected. A cadence of 0 indicates
     * that this value is unknown.
     */
    val cadence: Long
) {
    companion object {
        @JvmStatic
        fun toShardedResultGroup(shard: ClusterShard): (ResultGroup) -> ShardedResultGroup {
            return { ShardedResultGroup(shard.shard, it.key, it.series, it.group, it.cadence) }
        }

        @JvmStatic
        fun summarize(resultGroups: List<ResultGroup>): MultiSummary {
            val keySize = Histogram.Builder()
            val seriesSummarizer = SeriesSetsSummarizer()
            val dataSize = Histogram.Builder()
            var cadence = Optional.empty<Long>()

            resultGroups.forEach {
                keySize.add(it.key.size.toLong())
                seriesSummarizer.add(it.series)
                dataSize.add(it.group.size().toLong())
                cadence = Optional.of(it.cadence)
            }

            return MultiSummary(keySize.build(), seriesSummarizer.end(), dataSize.build(), cadence)
        }
    }

    data class MultiSummary(
        val keySize: Histogram,
        val series: SeriesSetsSummarizer.Summary,
        val dataSize: Histogram,
        val cadence: Optional<Long>
    )
}
