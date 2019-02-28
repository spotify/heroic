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

import com.spotify.heroic.ObjectHasher
import com.spotify.heroic.metric.MetricCollection
import com.spotify.heroic.metric.Point

/**
 * This filter strategy calculates the area under the graphs of the time series and
 * selects the time series with either the biggest (TopK) or smallest (BottomK) area.
 *
 *
 * Time series without any data points are disregarded and never part of the result.
 */
data class FilterKAreaStrategy(val filterType: FilterKAreaType, val k: Long) : FilterStrategy {
    private val limit = k.toInt()

    override fun <T> filter(metrics: List<FilterableMetrics<T>>): List<T?> {
        return metrics
            .asSequence()
            .filter { it.metricSupplier!!.get().size() > 0 }
            .map { Area(it) }
            .sortedWith(Comparator { a, b -> filterType.compare(a.value, b.value) })
            .take(limit)
            .map { it.filterableMetrics.data }
            .toList()
    }

    override fun hashTo(hasher: ObjectHasher) {
        hasher.putObject(javaClass) {
            hasher.putField("filterType", filterType, hasher.enumValue<FilterKAreaType>())
            hasher.putField("k", k, hasher.longValue())
        }
    }


    private data class Area<T>(val filterableMetrics: FilterableMetrics<T>, val value: Double) {
        constructor(filterableMetrics: FilterableMetrics<T>)
            : this(filterableMetrics, computeArea(filterableMetrics.metricSupplier!!.get()))

        companion object {
            fun computeArea(metricCollection: MetricCollection): Double {
                val metrics = metricCollection.getDataAs(Point::class.java)

                var area = 0.0
                for (i in 1 until metrics.size) {
                    area += PointPairArea.computeArea(metrics[i - 1], metrics[i])
                }

                return area
            }
        }
    }
}
