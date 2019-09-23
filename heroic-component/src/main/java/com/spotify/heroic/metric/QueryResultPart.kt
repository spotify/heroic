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

import com.spotify.heroic.aggregation.Aggregation
import com.spotify.heroic.aggregation.AggregationInstance
import com.spotify.heroic.cluster.ClusterShard
import eu.toolchain.async.Transform

data class QueryResultPart(
    /**
     * Groups of results.
     * <p>
     * Failed groups are omitted from here, {@link #errors} for these.
     */
    val groups: List<ShardedResultGroup>,

    /**
     * Errors that happened during the query.
     */
    val errors: List<RequestError>,

    /**
     * Query trace.
     */
    val queryTrace: QueryTrace,

    val limits: ResultLimits,
    val preAggregationSampleSize: Long
) {
    fun isEmpty() = groups.all { it.isEmpty() }

    companion object {
        @JvmStatic
        fun fromResultGroup(shard: ClusterShard): Transform<FullQuery, QueryResultPart> {
            return Transform { result: FullQuery ->
                val groupMapper = ResultGroup.toShardedResultGroup(shard)
                val groups = result.groups().map { groupMapper(it) }

                val preAggregationSampleSize = result
                    .statistics()
                    .getCounterValue(AggregationInstance.SAMPLE_SIZE)
                    .orElseGet {
                        var sum: Long = 0
                        groups.forEach { sum += it.metrics.data().size.toLong() }
                        sum
                    }

                QueryResultPart(groups, result.errors(), result.trace(), result.limits(),
                    preAggregationSampleSize)
            }
        }
    }
}
