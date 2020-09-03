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

import com.google.common.util.concurrent.AtomicDouble
import com.spotify.heroic.aggregation.AbstractBucket
import com.spotify.heroic.aggregation.DoubleBucket
import com.spotify.heroic.metric.Point
import com.spotify.heroic.metric.Spread

/**
 * A bucket implementation that retains the smallest (min) value seen.
 *
 * @author udoprog
 */
data class MinBucket(override val timestamp: Long) : AbstractBucket(), DoubleBucket {
    private val value = AtomicDouble(java.lang.Double.POSITIVE_INFINITY)

    override fun updatePoint(key: Map<String, String>, sample: Point) {
        while (true) {
            val current = value.get()

            if (current < sample.value) {
                break
            }

            if (value.compareAndSet(current, sample.value)) {
                break
            }
        }
    }

    override fun updateSpread(key: Map<String, String>, sample: Spread) {
        while (true) {
            val current = value.get()

            if (current < sample.min) {
                break
            }

            if (value.compareAndSet(current, sample.min)) {
                break
            }
        }
    }

    override fun value(): Double {
        val result = value.get()

        return if (!java.lang.Double.isFinite(result)) {
            java.lang.Double.NaN
        } else result

    }
}
