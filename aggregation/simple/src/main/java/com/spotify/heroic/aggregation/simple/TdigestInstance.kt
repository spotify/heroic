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

import com.google.common.collect.ImmutableSet
import com.spotify.heroic.aggregation.BucketAggregationInstance
import com.spotify.heroic.aggregation.TDigestBucket
import com.spotify.heroic.metric.Metric
import com.spotify.heroic.metric.MetricType
import com.spotify.heroic.metric.TdigestPoint


data class TdigestInstance (
        override val size: Long,
        override val extent: Long
) : BucketAggregationInstance<TDigestBucket>(
        size,
        extent,
        ImmutableSet.of(MetricType.DISTRIBUTION_POINTS, MetricType.TDIGEST_POINT),
        MetricType.TDIGEST_POINT) {

    override fun buildBucket(timestamp: Long): TdigestMergingBucket {
        return TdigestMergingBucket(timestamp)
    }

    override fun build(bucket: TDigestBucket): Metric {
       if ( bucket.value().size() == 0L ) return Metric.invalid
        else return TdigestPoint.create(bucket.value(), bucket.timestamp)
    }
}

