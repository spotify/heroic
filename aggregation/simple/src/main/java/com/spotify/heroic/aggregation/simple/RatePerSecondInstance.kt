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

import com.google.common.collect.ImmutableSet
import com.google.common.util.concurrent.AtomicDouble
import com.spotify.heroic.aggregation.AbstractBucket
import com.spotify.heroic.aggregation.AggregationInstance
import com.spotify.heroic.aggregation.BucketAggregationInstance
import com.spotify.heroic.aggregation.DoubleBucket
import com.spotify.heroic.metric.Metric
import com.spotify.heroic.metric.MetricType
import com.spotify.heroic.metric.Point

/*
    Calculate the rate per second of cumulative counters. When the counter is reset
    because of an instance restart the interval will be skipped.
 */
data class RatePerSecondInstance(
    override val size: Long,
    override val extent: Long
) : BucketAggregationInstance<RatePerSecondInstance.RateBucket>(size,
    extent,
    ImmutableSet.of(MetricType.POINT),
    MetricType.POINT
) {
    override fun distributable(): Boolean {
        return false
    }

    override fun buildBucket(timestamp: Long): RateBucket {
        return RateBucket(timestamp)
    }

    override fun reducer(): AggregationInstance {
        return SumInstance(size, extent)
    }

    override fun build(bucket: RateBucket): Metric {
        val value = bucket.value()

        return if (java.lang.Double.isNaN(value)) {
            Metric.invalid
        } else Point(bucket.timestamp, value)
    }

    data class RateBucket(override val timestamp: Long) : AbstractBucket(), DoubleBucket {
        private var firstPoint: Point? = null
        private var lastPoint: Point? = null
        private val rate = AtomicDouble()

        override fun updatePoint(key: Map<String, String>, sample: Point) {
            if (firstPoint == null) {
                firstPoint = sample
            }

            // Don't produce a negative value when a monotonically increasing counter is reset.
            if (sample.value > lastPoint?.value ?: sample.value) {
                rate.addAndGet(sample.value - lastPoint!!.value)
            }
            lastPoint = sample
        }

        override fun value(): Double {
            val timeDiff = (lastPoint?.timestamp ?: 0) - (firstPoint?.timestamp ?: 0)
            return (rate.toDouble()/timeDiff * 1000)
        }
    }

}
