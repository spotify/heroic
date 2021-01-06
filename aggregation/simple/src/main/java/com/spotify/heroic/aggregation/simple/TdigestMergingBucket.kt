/*
 * Copyright (c) 2020 Spotify AB.
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
import com.spotify.heroic.aggregation.TDigestBucket
import com.spotify.heroic.metric.DistributionPoint
import com.spotify.heroic.metric.HeroicDistribution
import com.spotify.heroic.metric.TdigestPoint
import com.tdunning.math.stats.TDigest;
import java.util.concurrent.atomic.AtomicReference



data class TdigestMergingBucket(override val timestamp: Long) : AbstractBucket(), TDigestBucket {
    private val datasketch : AtomicReference<TDigest> = TdigestStatInstanceUtils.buildAtomicReference()


    override fun updateDistributionPoint(key: Map<String, String>, sample : DistributionPoint) {
        val heroicDistribution : HeroicDistribution = HeroicDistribution.create(sample.value().value)
        val serializedDatasketch = heroicDistribution.toByteBuffer()
        datasketch.getAndUpdate(TdigestStatInstanceUtils.getOp(serializedDatasketch))
    }

    override fun updateTDigestPoint(key: Map<String, String>, sample : TdigestPoint) {
        datasketch.getAndUpdate(TdigestStatInstanceUtils.getOp(sample.value()))
    }

    override fun value(): TDigest {
        return datasketch.get()
    }
}
