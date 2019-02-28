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
import java.util.concurrent.atomic.AtomicLong

data class AverageBucket(override val timestamp: Long = 0) : AbstractBucket(), DoubleBucket {
    val value = AtomicDouble()
    val count = AtomicLong()

    override fun updatePoint(key: Map<String, String>, sample: Point) {
        value.addAndGet(sample.value)
        count.incrementAndGet()
    }

    override fun updateSpread(key: Map<String, String>, sample: Spread) {
        value.addAndGet(sample.sum)
        count.addAndGet(sample.count)
    }

    override fun value(): Double {
        val count = this.count.get()

        return if (count == 0L) {
            java.lang.Double.NaN
        } else value.get() / count

    }
}
