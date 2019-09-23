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

package com.spotify.heroic.metric

import com.google.common.hash.Hasher

data class Spread(
    override val timestamp: Long,
    val count: Long,
    val sum: Double,
    val sum2: Double,
    val min: Double,
    val max: Double
): Metric {
    override fun valid() = true

    override fun hash(hasher: Hasher) {
        hasher.putInt(MetricType.SPREAD.ordinal)
        hasher.putLong(timestamp)
        hasher.putLong(count)
        hasher.putDouble(sum)
        hasher.putDouble(sum2)
        hasher.putDouble(min)
        hasher.putDouble(max)
    }
}