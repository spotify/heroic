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

abstract class MetricMappingAggregation(
    @JsonIgnore val metricMappingStrategy: MetricMappingStrategy
) : AggregationInstance {

    override fun estimate(range: DateRange): Long {
        return INNER.estimate(range)
    }

    override fun cadence(): Long {
        return 0
    }

    override fun session(
        range: DateRange, quotaWatcher: RetainQuotaWatcher, bucketStrategy: BucketStrategy
    ): AggregationSession {
        return Session(INNER.session(range, quotaWatcher, bucketStrategy))
    }

    override fun distributed(): AggregationInstance {
        return INNER
    }

    override fun hashTo(hasher: ObjectHasher) {
        hasher.putObject(javaClass) {
            hasher.putField("metricMappingStrategy", metricMappingStrategy,
                hasher.with(MetricMappingStrategy::hashTo))
        }
    }

    internal inner class Session(private val childSession: AggregationSession) : AggregationSession {

        override fun updatePoints(
            key: Map<String, String>, series: Set<Series>, values: List<Point>
        ) {
            this.childSession.updatePoints(key, series, values)
        }

        override fun updatePayload(
            key: Map<String, String>, series: Set<Series>, values: List<Payload>
        ) {
            this.childSession.updatePayload(key, series, values)
        }

        override fun updateGroup(
            key: Map<String, String>, series: Set<Series>, values: List<MetricGroup>
        ) {
            this.childSession.updateGroup(key, series, values)
        }

        override fun updateSpreads(
            key: Map<String, String>, series: Set<Series>,
            values: List<com.spotify.heroic.metric.Spread>
        ) {
            this.childSession.updateSpreads(key, series, values)
        }

        override fun updateDistributionPoints(
                key: Map<String, String>, series: Set<Series>,
                values: List<DistributionPoint>
        ) {
            this.childSession.updateDistributionPoints(key, series, values)
        }

        override fun updateTDigestPoints(
                key: Map<String, String>, series: Set<Series>,
                values: List<TdigestPoint>
        ) {
            this.childSession.updateTDigestPoints(key, series, values)
        }

        override fun result(): AggregationResult {
            val (result, statistics) = this.childSession.result()
            val outputs = result
                .map { (key, series, metrics) ->
                    AggregationOutput(key, series, metricMappingStrategy.apply(metrics))
                }
            return AggregationResult(outputs, statistics)
        }
    }

    companion object {
        private val INNER = EmptyInstance.INSTANCE
    }
}
