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

import com.spotify.heroic.ObjectHasher
import com.spotify.heroic.aggregation.*
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.Series
import com.spotify.heroic.metric.*
import com.spotify.heroic.metric.Spread
import java.util.*

object NotNegativeInstance : AggregationInstance {
    private val INNER = EmptyInstance.INSTANCE

    override fun estimate(range: DateRange): Long {
        return INNER.estimate(range)
    }

    override fun cadence(): Long {
        return -1
    }

    override fun distributed(): AggregationInstance {
        return this
    }

    override fun reducer(): AggregationInstance {
        return INNER
    }

    override fun distributable(): Boolean {
        return false
    }

    override fun hashTo(hasher: ObjectHasher) {
        hasher.putObject(javaClass)
    }

    fun filterPoints(points: List<Point>): List<Point> {
        val it = points.iterator()
        val result = ArrayList<Point>()

        if (!it.hasNext()) {
            return emptyList()
        }

        while (it.hasNext()) {
            val point = it.next()

            if (point.value >= 0.0) {
                result.add(point)
            }
        }

        return result
    }

    override fun session(
        range: DateRange, quotaWatcher: RetainQuotaWatcher, bucketStrategy: BucketStrategy
    ): AggregationSession {
        return Session(INNER.session(range, quotaWatcher, bucketStrategy))
    }

    private class Session(private val childSession: AggregationSession) : AggregationSession {

        override fun updatePoints(
            key: Map<String, String>, series: Set<Series>, values: List<Point>
        ) {
            this.childSession.updatePoints(key, series, values)
        }

        override fun updatePayload(
            key: Map<String, String>, series: Set<Series>, values: List<Payload>
        ) {
        }

        override fun updateGroup(
            key: Map<String, String>, series: Set<Series>, values: List<MetricGroup>
        ) {
        }

        override fun updateSpreads(
            key: Map<String, String>, series: Set<Series>, values: List<Spread>
        ) {
        }

        override fun updateDistributionPoints(
                key: Map<String, String>, series: Set<Series>, values: List<DistributionPoint>
        ) {
        }

        override fun result(): AggregationResult {
            val (result, statistics) = this.childSession.result()
            val outputs = result
                .map { (key, series, metrics) ->
                    AggregationOutput(
                        key,
                        series,
                        MetricCollection.build(
                            MetricType.POINT,
                            filterPoints(metrics.getDataAs(Point::class.java))
                        )
                    )
                }
            return AggregationResult(outputs, statistics)
        }
    }
}
