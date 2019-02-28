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

package com.spotify.heroic.aggregation.cardinality

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.spotify.heroic.metric.Metric
import com.spotify.heroic.metric.Payload

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Bucket that counts the number of seen events.
 *
 * @author udoprog
 */
data class ReduceHyperLogLogPlusCardinalityBucket(
    override val timestamp: Long
) : CardinalityBucket {

    private val states = ConcurrentLinkedQueue<HyperLogLogPlus>()

    override fun updatePayload(
            key: Map<String, String>, sample: Payload
    ) {
        try {
            states.add(HyperLogLogPlus.Builder.build(sample.state()))
        } catch (e: IOException) {
            throw RuntimeException("Failed to deserialize state", e)
        }

    }

    override fun update(key: Map<String, String>, d: Metric) {}

    override fun count(): Long {
        val it = states.iterator()

        if (!it.hasNext()) {
            return 0L
        }

        val current = it.next()

        while (it.hasNext()) {
            try {
                current.addAll(it.next())
            } catch (e: CardinalityMergeException) {
                throw RuntimeException(e)
            }

        }

        return current.cardinality()
    }
}
