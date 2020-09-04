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

import com.spotify.heroic.aggregation.AbstractBucket
import com.spotify.heroic.metric.Metric
import com.spotify.heroic.metric.Point
import com.spotify.heroic.metric.Spread
import java.util.concurrent.atomic.DoubleAccumulator
import java.util.concurrent.atomic.DoubleAdder
import java.util.concurrent.atomic.LongAdder

/**
 * Bucket that keeps track of the amount of data points seen, and there summed value.
 *
 *
 * Take care to not blindly trust [.value] since it is initialized to 0 for simplicity.
 * Always check [.count], which if zero indicates that the [.value] is undefined
 * (e.g. NaN).
 *
 * @author udoprog
 */
data class SpreadBucket(override val timestamp: Long) : AbstractBucket() {

    internal val count = LongAdder()
    internal val sum = DoubleAdder()
    internal val sum2 = DoubleAdder()
    internal val max = DoubleAccumulator(maxFn, java.lang.Double.NEGATIVE_INFINITY)
    internal val min = DoubleAccumulator(minFn, java.lang.Double.POSITIVE_INFINITY)

    override fun updateSpread(key: Map<String, String>, sample: Spread) {
        count.add(sample.count)
        sum.add(sample.sum)
        sum2.add(sample.sum2)
        max.accumulate(sample.max)
        min.accumulate(sample.min)
    }

    override fun updatePoint(key: Map<String, String>, sample: Point) {
        val value = sample.value

        if (!java.lang.Double.isFinite(value)) {
            return
        }

        count.increment()
        sum.add(value)
        sum2.add(value * value)
        max.accumulate(value)
        min.accumulate(value)
    }

    fun newSpread(): Metric {
        val count = this.count.sum()

        return if (count == 0L) {
            Metric.invalid
        } else Spread(timestamp, count, sum.sum(), sum2.sum(), min.get(), max.get())

    }

    companion object {
        internal val minFn = { left: Double, right: Double -> Math.min(left, right) }
        internal val maxFn = { left: Double, right: Double -> Math.max(left, right) }
    }
}
