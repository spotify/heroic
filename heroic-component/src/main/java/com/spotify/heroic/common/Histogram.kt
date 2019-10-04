/*
 * Copyright (c) 2019 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"): you may not use this file except in compliance
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

package com.spotify.heroic.common

import com.google.common.collect.TreeMultiset
import java.util.*
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

/**
 * Utility class to build basic statistics about quantities.
 */
data class Histogram(
    val median: Optional<Long>,
    val p75: Optional<Long>,
    val p99: Optional<Long>,
    val min: Optional<Long>,
    val max: Optional<Long>,
    val mean: Optional<Double>,
    val sum: Optional<Long>
) {
    companion object {
        @JvmStatic
        fun empty() = Builder().build()
    }

    class Builder(
        val samples: TreeMultiset<Long> = TreeMultiset.create(Long::compareTo)
    ) {
        fun add(sample: Long) {
            samples.add(sample)
        }

        fun build(): Histogram {
            val entries = samples.toList()
            val mean = meanAndSum(entries)

            return Histogram(
                median = nth(entries, 0.50),
                p75 = nth(entries, 0.75),
                p99 = nth(entries, 0.99),
                min = nth(entries, 0.00),
                max = nth(entries, 1.00),
                mean = mean.first,
                sum = mean.second
            )
        }

        private fun meanAndSum(entries: List<Long>)
            : Pair<Optional<Double>, Optional<Long>> {

            val count = entries.size

            if (count <= 0) return Optional.empty<Double>() to Optional.empty()

            val sum = entries.reduce { a, b -> a + b }
            val mean = sum.toDouble() / count.toDouble()
            return Optional.of(mean) to Optional.of(sum)
        }

        /**
         * Simplified accessor for n.
         */
        private fun nth(samples: List<Long>, n: Double): Optional<Long> {
            if (samples.isEmpty()) return Optional.empty()

            val index = min(samples.size - 1,
                max(0, (samples.size.toDouble() * n).roundToInt() - 1)
            )

            return Optional.ofNullable(samples[index])
        }
    }
}
