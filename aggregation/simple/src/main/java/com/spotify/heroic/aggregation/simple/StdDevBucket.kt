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
import java.util.concurrent.atomic.AtomicReference

/**
 * Bucket that calculates the standard deviation of all buckets seen.
 *
 *
 * This uses Welford's method, as presented in http://www.johndcook.com/blog/standard_deviation/
 *
 * @author udoprog
 */
data class StdDevBucket(override val timestamp: Long) : AbstractBucket(), DoubleBucket {
    private val cell = AtomicReference(ZERO)

    override fun updatePoint(key: Map<String, String>, sample: Point) {
        val value = sample.value

        while (true) {
            val c = cell.get()

            val count = c.count + 1
            val delta = value - c.mean
            val mean = c.mean + delta / count
            val s = c.s + delta * (value - mean)

            val n = Cell(mean, s, count)

            if (cell.compareAndSet(c, n)) {
                break
            }
        }
    }

    override fun value(): Double {
        val c = cell.get()

        return if (c.count <= 1) {
            java.lang.Double.NaN
        } else Math.sqrt(c.s / (c.count - 1))

    }

    private data class Cell(val mean: Double, val s: Double, val count: Long)

    companion object {
        private val ZERO = Cell(0.0, 0.0, 0)
    }
}
