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
import com.spotify.heroic.aggregation.DoubleBucket
import com.spotify.heroic.metric.Point
import com.spotify.heroic.metric.Spread
import java.util.concurrent.atomic.DoubleAdder
import java.util.concurrent.atomic.LongAdder

/**
 * A lock-free implementation for calculating the standard deviation over many values.
 *
 *
 * This bucket uses primitives based on striped atomic updates to reduce contention across CPUs.
 *
 * @author udoprog
 */
data class StripedStdDevBucket(override val timestamp: Long) : AbstractBucket(), DoubleBucket {
    private val sum = DoubleAdder()
    private val sum2 = DoubleAdder()
    private val count = LongAdder()

    override fun updateSpread(key: Map<String, String>, sample: Spread) {
        sum.add(sample.sum)
        sum2.add(sample.sum2)
        count.add(sample.count)
    }

    override fun updatePoint(key: Map<String, String>, sample: Point) {
        val v = sample.value

        sum.add(v)
        sum2.add(v * v)
        count.increment()
    }

    override fun value(): Double {
        val count = this.count.sum()

        if (count == 0L) {
            return java.lang.Double.NaN
        }

        val sum = this.sum.sum()
        val sum2 = this.sum2.sum()
        val mean = sum / count

        return Math.sqrt(sum2 / count - mean * mean)
    }
}
