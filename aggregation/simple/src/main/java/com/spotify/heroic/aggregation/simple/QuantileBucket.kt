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

// THIS COMPONENT WAS ADAPTED FROM THE HADOOP PROJECT:
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.spotify.heroic.aggregation.simple

import com.spotify.heroic.aggregation.AbstractBucket
import com.spotify.heroic.metric.Point
import java.util.Arrays
import java.util.LinkedList

/**
 * Implementation of the Cormode, Korn, Muthukrishnan, and Srivastava algorithm for streaming
 * calculation of targeted high-percentile epsilon-approximate quantiles.
 *
 *
 * This is a generalization of the earlier work by Greenwald and Khanna (GK), which essentially
 * allows different error bounds on the targeted quantiles, which allows for far more efficient
 * calculation of high-percentiles.
 *
 *
 * See: Cormode, Korn, Muthukrishnan, and Srivastava "Effective Computation of Biased Quantiles over
 * Data Streams" in ICDE 2005
 *
 *
 * Greenwald and Khanna, "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 */
data class QuantileBucket(
    override val timestamp: Long,
    val quantile: Double,
    val error: Double
) : AbstractBucket() {

    /**
     * Set of active samples.
     */
    private val samples = LinkedList<SampleItem>()

    /**
     * Total count of samples gathered.
     */
    private var count: Long = 0

    /**
     * Current batch to insert and the corresponding write index.
     */
    private val batch = DoubleArray(500)
    private var index = 0

    val sampleSize: Int
        @Synchronized get() = samples.size

    /**
     * Add a new data point from the stream.
     *
     * @param d data point to add.
     */
    @Synchronized
    override fun updatePoint(key: Map<String, String>, sample: Point) {
        batch[index] = sample.value
        index++
        count++

        if (index == batch.size) {
            compact()
        }
    }

    @Synchronized
    fun value(): Double {
        if (index > 0) {
            compact()
        }

        return query(quantile)
    }

    private fun compact() {
        insertBatch()
        compressSamples()
    }

    /**
     * Merges items from buffer into the samples array in one pass. This is more efficient than
     * doing an insert on every item.
     */
    private fun insertBatch() {
        if (index == 0) {
            return
        }

        Arrays.sort(batch, 0, index)

        // Base case: no samples
        var start = 0

        if (samples.isEmpty()) {
            samples.add(SampleItem(batch[0], 0, 1))
            start++
        }

        val it = samples.listIterator()

        var prev = it.next()

        for (i in start until index) {
            val value = batch[i]

            while (it.nextIndex() < samples.size && prev.value < value) {
                prev = it.next()
            }

            // If we found that bigger item, back up so we insert ourselves before it
            if (prev.value > value) {
                it.previous()
            }

            // We use different indexes for the edge comparisons, because of the above
            // if statement that adjusts the iterator
            val delta = calculateDelta(it.previousIndex(), it.nextIndex())

            val next = SampleItem(value, delta, 1)
            it.add(next)
            prev = next
        }

        index = 0
    }

    /**
     * Try to remove extraneous items from the set of sampled items. This checks if an item is
     * unnecessary based on the desired error bounds, and merges it with the adjacent item if it
     * is.
     */
    private fun compressSamples() {
        if (samples.size < 2) {
            return
        }

        val it = samples.listIterator()

        var next = it.next()

        while (it.hasNext()) {
            val prev = next

            next = it.next()

            if (prev.g + next.g + next.delta > allowableError(it.previousIndex())) {
                continue
            }

            next.g += prev.g

            // Remove prev. it.remove() kills the last thing returned.
            it.previous()
            it.previous()
            it.remove()

            // it.next() is now equal to next, skip it back forward again
            it.next()
        }
    }

    /**
     * Specifies the allowable error for this rank, depending on which quantiles are being
     * targeted.
     *
     *
     * This is the f(r_i, n) function from the CKMS paper. It's basically how wide the range of this
     * rank can be.
     *
     * @param rank the index in the list of samples
     */
    private fun allowableError(rank: Int): Double {
        val size = samples.size

        val error = calculateError(rank, size)
        val minError = (size + 1).toDouble()

        return if (error < minError) {
            error
        } else minError

    }

    private fun calculateError(rank: Int, size: Int): Double {
        return if (rank <= quantile * size) {
            2.0 * this.error * (size - rank).toDouble() / (1.0 - quantile)
        } else 2.0 * this.error * rank.toDouble() / quantile

    }

    private fun calculateDelta(previousIndex: Int, nextIndex: Int): Int {
        return if (previousIndex == 0 || nextIndex == samples.size) {
            0
        } else Math.floor(allowableError(nextIndex)).toInt() - 1

    }

    /**
     * Get the estimated value at the specified quantile.
     *
     * @param quantile Queried quantile, e.g. 0.50 or 0.99.
     * @return Estimated value at that quantile.
     */
    private fun query(quantile: Double): Double {
        if (samples.isEmpty()) {
            throw IllegalStateException("no data in estimator")
        }

        var rankMin = 0
        val desired = (quantile * count).toInt()

        val it = samples.listIterator()

        var next = it.next()

        for (i in 1 until samples.size) {
            val prev = next

            next = it.next()

            rankMin += prev.g

            if (rankMin + next.g + next.delta > desired + allowableError(i) / 2) {
                return prev.value
            }
        }

        // edge case of wanting max value
        return samples[samples.size - 1].value
    }

    private data class SampleItem(val value: Double, val delta: Int, var g: Int)
}
