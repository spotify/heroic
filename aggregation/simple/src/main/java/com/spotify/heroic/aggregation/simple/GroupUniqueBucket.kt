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

import com.google.common.collect.ImmutableList
import com.spotify.heroic.aggregation.AbstractBucket
import com.spotify.heroic.aggregation.Bucket
import com.spotify.heroic.metric.*
import com.spotify.heroic.metric.Spread
import java.util.*
import java.util.concurrent.ConcurrentSkipListSet

data class GroupUniqueBucket(override val timestamp: Long) : AbstractBucket(), Bucket {
    internal val points: SortedSet<Point> = ConcurrentSkipListSet(Metric.comparator)
    internal val spreads: SortedSet<Spread> = ConcurrentSkipListSet(Metric.comparator)
    internal val groups: SortedSet<MetricGroup> = ConcurrentSkipListSet(Metric.comparator)

    fun groups(): List<MetricCollection> {
        val result = ImmutableList.builder<MetricCollection>()

        if (!points.isEmpty()) {
            result.add(MetricCollection.points(ImmutableList.copyOf(points)))
        }

        if (!spreads.isEmpty()) {
            result.add(MetricCollection.spreads(ImmutableList.copyOf(spreads)))
        }

        if (!groups.isEmpty()) {
            result.add(MetricCollection.groups(ImmutableList.copyOf(groups)))
        }

        return result.build()
    }

    override fun updatePoint(key: Map<String, String>, sample: Point) {
        points.add(sample)
    }

    override fun updateSpread(key: Map<String, String>, sample: Spread) {
        spreads.add(sample)
    }

    override fun updateGroup(key: Map<String, String>, sample: MetricGroup) {
        groups.add(sample)
    }
}
