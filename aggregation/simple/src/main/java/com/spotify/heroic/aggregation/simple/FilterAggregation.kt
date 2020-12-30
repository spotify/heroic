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

package com.spotify.heroic.aggregation.simple

import com.fasterxml.jackson.annotation.JsonIgnore
import com.spotify.heroic.ObjectHasher
import com.spotify.heroic.aggregation.*
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.Series
import com.spotify.heroic.metric.*
import com.spotify.heroic.metric.Spread
import java.util.function.Supplier

abstract class FilterAggregation(
    @JsonIgnore val filterStrategy: FilterStrategy
) : AggregationInstance {

    override fun estimate(range: DateRange): Long {
        return INNER.estimate(range)
    }

    override fun cadence(): Long {
        return -1
    }

    override fun distributed(): AggregationInstance {
        return INNER
    }

    override fun reducer(): AggregationInstance {
        return INNER
    }

    /**
     * Filtering aggregations are by definition *not* distributable since they are incapable
     * of making a complete local decision.
     */
    override fun distributable(): Boolean {
        return false
    }

    override fun session(
        range: DateRange, quotaWatcher: RetainQuotaWatcher, bucketStrategy: BucketStrategy
    ): AggregationSession {
        return Session(filterStrategy, INNER.session(range, quotaWatcher, bucketStrategy))
    }

    protected abstract fun filterHashTo(hasher: ObjectHasher)

    override fun hashTo(hasher: ObjectHasher) {
        hasher.putObject(javaClass) {
            hasher.putField("filterStrategy", filterStrategy, hasher.with(FilterStrategy::hashTo))
            filterHashTo(hasher)
        }
    }

    private inner class Session(
        private val filterStrategy: FilterStrategy,
        private val childSession: AggregationSession
    ) : AggregationSession {

        override fun updatePoints(
            key: Map<String, String>, series: Set<Series>, values: List<Point>
        ) {
            childSession.updatePoints(key, series, values)
        }

        override fun updateSpreads(
            key: Map<String, String>, series: Set<Series>, values: List<Spread>
        ) {
            childSession.updateSpreads(key, series, values)
        }

        override fun updateGroup(
            key: Map<String, String>, series: Set<Series>, values: List<MetricGroup>
        ) {
            childSession.updateGroup(key, series, values)
        }

        override fun updatePayload(
            key: Map<String, String>, series: Set<Series>, values: List<Payload>
        ) {
            childSession.updatePayload(key, series, values)
        }

        override fun updateDistributionPoints(
                key: Map<String, String>, series: Set<Series>, values: List<DistributionPoint>
        ) {
            childSession.updateDistributionPoints(key, series, values)
        }

        override fun result(): AggregationResult {
            val result = childSession.result()

            val filterable = result
                .result
                .map { FilterableMetrics(it, Supplier { it.metrics } ) }

            return AggregationResult(filterStrategy.filter(filterable), result.statistics)
        }
    }

    companion object {
        private val INNER = EmptyInstance.INSTANCE
    }
}
