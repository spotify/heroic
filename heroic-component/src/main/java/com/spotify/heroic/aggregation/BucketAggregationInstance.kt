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

package com.spotify.heroic.aggregation

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.spotify.heroic.ObjectHasher
import com.spotify.heroic.aggregation.BucketStrategy.Mapping
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.Series
import com.spotify.heroic.common.Statistics
import com.spotify.heroic.metric.*
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.LongAdder

/**
 * A base aggregation that collects data in 'buckets', one for each sampled data point.
 *
 *
 * A bucket aggregation is used to down-sample a lot of data into distinct buckets over time, making
 * them useful for presentation purposes. Buckets have to be thread safe.
 *
 * @param <B> The bucket type.
 * @author udoprog
 * @see Bucket
</B> */
abstract class BucketAggregationInstance<B : Bucket>(
    open val size: Long,
    open val extent: Long,
    private val input: Set<MetricType>,
    protected val out: MetricType
) : AggregationInstance {

    inner class Session(val mapping: Mapping, val buckets: List<B>) : AggregationSession {
        val series: ConcurrentLinkedQueue<Set<Series>> = ConcurrentLinkedQueue()
        private val sampleSize: LongAdder = LongAdder()

        override fun updatePoints(
            key: Map<String, String>, s: Set<Series>, values: List<Point>
        ) {
            series.add(s)
            feed(MetricType.POINT, values,
                { bucket, m -> bucket.updatePoint(key, m as Point) })
        }

        override fun updateDistributionPoints(
                key: Map<String, String>, s: Set<Series>, values: List<DistributionPoint>
        ) {
            series.add(s)
            feed(MetricType.DISTRIBUTION_POINTS, values,
                    { bucket, m -> bucket.updateDistributionPoint(key, m as DistributionPoint) })
        }

        override fun updateSpreads(
            key: Map<String, String>, s: Set<Series>, values: List<Spread>
        ) {
            series.add(s)
            feed(MetricType.SPREAD, values,
                { bucket, m -> bucket.updateSpread(key, m as Spread) })
        }


        override fun updateTDigestPoints(
                key: Map<String, String>, s: Set<Series>, values: List<TdigestPoint>
        ) {
            series.add(s)
            feed(MetricType.TDIGEST_POINT, values,
                    { bucket, m -> bucket.updateTDigestPoint(key, m as TdigestPoint) })

        }

        override fun updateGroup(
            key: Map<String, String>, s: Set<Series>, values: List<MetricGroup>
        ) {
            series.add(s)
            feed(MetricType.GROUP, values,
                { bucket, m -> bucket.updateGroup(key, m as MetricGroup) })
        }

        override fun updatePayload(
            key: Map<String, String>, s: Set<Series>, values: List<Payload>
        ) {
            series.add(s)
            feed(MetricType.CARDINALITY, values,
                { bucket, m -> bucket.updatePayload(key, m as Payload) })

        }



        private fun <T : Metric> feed(
            type: MetricType, values: List<T>, consumer: (B : Bucket, T : Metric) -> Unit
        ) {
            if (!input.contains(type)) {
                return
            }

            var sampleSize = 0

            for (m in values.filter { it.valid() }) {
                val startEnd = mapping.map(m.timestamp)

                for (i in startEnd.start until startEnd.end) {
                    consumer(buckets[i], m)
                }

                sampleSize += 1
            }

            this.sampleSize.add(sampleSize.toLong())
        }

        override fun result(): AggregationResult {
            val result = ArrayList<Metric>(buckets.size)

            for (bucket in buckets) {
                val d = build(bucket)

                if (!d.valid()) {
                    continue
                }

                result.add(d)
            }

            val series = ImmutableSet.copyOf(Iterables.concat(this.series))
            val metrics = MetricCollection.build(out, result)

            val statistics = Statistics(ImmutableMap.of(AggregationInstance.SAMPLE_SIZE, sampleSize.sum()))

            val d = AggregationOutput(EMPTY_KEY, series, metrics)
            return AggregationResult(ImmutableList.of(d), statistics)
        }
    }

    override fun estimate(original: DateRange): Long {
        return if (size == 0L) {
            0
        } else original.rounded(size).diff() / size

    }

    override fun session(
        range: DateRange, quotaWatcher: RetainQuotaWatcher, bucketStrategy: BucketStrategy
    ): Session {
        val mapping = bucketStrategy.setup(range, size, extent)
        val buckets = buildBuckets(mapping)
        quotaWatcher.retainData(buckets.size.toLong())
        return Session(mapping, buckets)
    }

    override fun distributed(): AggregationInstance {
        return this
    }

    override fun cadence(): Long {
        return size
    }

    protected open fun bucketHashTo(hasher: ObjectHasher) {}

    override fun hashTo(hasher: ObjectHasher) {
        hasher.putObject(javaClass) {
            hasher.putField("size", size, hasher.longValue())
            hasher.putField("extent", extent, hasher.longValue())
            bucketHashTo(hasher)
        }
    }

    override fun toString(): String {
        return String.format("%s(size=%d, extent=%d)", javaClass.simpleName, size, extent)
    }

    private fun buildBuckets(mapping: BucketStrategy.Mapping): List<B> {
        val buckets = ArrayList<B>(mapping.buckets())

        for (i in 0 until mapping.buckets()) {
            buckets.add(buildBucket(mapping.start() + size * i))
        }

        return buckets
    }

    protected abstract fun buildBucket(timestamp: Long): B

    protected abstract fun build(bucket: B): Metric

    companion object {
        @JvmField val EMPTY_KEY: Map<String, String> = ImmutableMap.of()
        @JvmField val ALL_TYPES: Set<MetricType> = ImmutableSet.of(
            MetricType.POINT, MetricType.SPREAD, MetricType.GROUP,
            MetricType.CARDINALITY, MetricType.DISTRIBUTION_POINTS, MetricType.TDIGEST_POINT)
    }
}
