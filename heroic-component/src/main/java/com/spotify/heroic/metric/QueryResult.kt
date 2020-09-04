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

import com.spotify.heroic.aggregation.AggregationCombiner
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.OptionalLimit
import eu.toolchain.async.Collector
import java.util.*

data class QueryResult(
    /**
     * The range in which all result groups metric's should be contained in.
     */
    val range: DateRange,

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
     * Query trace, if available.
     */
    val trace: QueryTrace,

    val limits: ResultLimits,

    /**
     * Number of raw data points before any aggregations are applied.
     */
    val preAggregationSampleSize: Long,

    /**
     * Extra information about caching.
     */
    val cache: Optional<CacheInfo>
) {
    /**
     * Add cache info to the result.
     * @param cache cache info to add.
     * @return an copied instanceof query result with cache info added
     */
    fun withCache(cache: CacheInfo): QueryResult {
        return QueryResult(range, groups, errors, trace, limits, preAggregationSampleSize,
            Optional.of(cache))
    }

    companion object {
        /**
         * Collect result parts into a complete result.
         *
         * @param range The range which the result represents.
         * @return A complete QueryResult.
         */
        @JvmStatic
        fun collectParts(
            what: QueryTrace.Identifier,
            range: DateRange,
            combiner: AggregationCombiner,
            groupLimit: OptionalLimit
        ): Collector<QueryResultPart, QueryResult> {
            val w = Tracing.DEFAULT.watch(what)
            return Collector { parts: Collection<QueryResultPart> ->
                val all = mutableListOf<List<ShardedResultGroup>>()
                val errors = mutableListOf<RequestError>()
                val queryTraces = mutableListOf<QueryTrace>()
                val limits = mutableListOf<ResultLimit>()
                var preAggregationSampleSize: Long = 0

                parts.asSequence().map {
                    errors.addAll(it.errors)
                    queryTraces.add(it.queryTrace)
                    limits.addAll(it.limits.limits)
                    preAggregationSampleSize += it.preAggregationSampleSize
                    it
                }.filterNot { it.isEmpty() }.forEach { all.add(it.groups) }

                val groups = combiner.combine(all)
                val trace = w.end(queryTraces.toList())
                if (groupLimit.isGreaterOrEqual(groups.size.toLong())) {
                    limits.add(ResultLimit.GROUP)
                }

                QueryResult(
                    range,
                    groupLimit.limitList(groups),
                    errors,
                    trace,
                    ResultLimits(limits.toSet()),
                    preAggregationSampleSize,
                    Optional.empty()
                )
            }
        }

        @JvmStatic
        fun error(range: DateRange, errorMessage: String, trace: QueryTrace): QueryResult =
            QueryResult(range, emptyList(), listOf(QueryError(errorMessage)), trace,
                ResultLimits.of(), 0, Optional.empty())
    }
}
