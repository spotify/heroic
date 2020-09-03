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

import com.google.common.base.Charsets
import com.google.common.collect.Ordering
import com.google.common.hash.HashCode
import com.google.common.hash.Hashing
import com.spotify.heroic.metric.Metric
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.Boolean
import kotlin.ByteArray
import kotlin.Comparator
import kotlin.Long
import kotlin.RuntimeException
import kotlin.String

/**
 * Bucket that counts the number of seen events.
 *
 * @author udoprog
 */
data class ExactCardinalityBucket(
    override val timestamp: Long,
    private val includeKey: Boolean
) : CardinalityBucket {

    private val count = AtomicInteger(0)
    private val seen = Collections.newSetFromMap(ConcurrentHashMap<HashCode, Boolean>())

    override fun update(key: Map<String, String>, d: Metric) {
        val hasher = HASH_FUNCTION.newHasher()

        if (includeKey) {
            for (k in KEY_ORDER.sortedCopy(key.keys)) {
                hasher.putString(k, Charsets.UTF_8).putString(key[k].orEmpty(), Charsets.UTF_8)
            }
        }

        d.hash(hasher)

        if (seen.add(hasher.hash())) {
            count.incrementAndGet()
        }
    }

    override fun count(): Long {
        return count.get().toLong()
    }

    override fun state(): ByteArray {
        throw RuntimeException("Bucket does not support state persisting")
    }

    companion object {
        private val HASH_FUNCTION = Hashing.goodFastHash(128)
        private val KEY_ORDER = Ordering.from(Comparator<String> { obj, s -> obj.compareTo(s) })
    }
}
