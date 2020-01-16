/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.aggregation.BucketAggregationInstance;
import com.spotify.heroic.metric.MetricType;

import java.util.Set;

/*
   Any aggregation using this bucket will have its updateSpreads method called given
   distributed() is overridden to return Spreads and  "Set<MetricType> input" includes it.
 */
public abstract class DistributedBucketInstance<B extends Bucket>
    extends BucketAggregationInstance<B> {
    public DistributedBucketInstance(
        final long size, final long extent, final Set<MetricType> input, final MetricType out
    ) {
        super(size, extent,
            ImmutableSet.<MetricType>builder().addAll(input).add(MetricType.SPREAD).build(), out);
    }

    @Override
    public AggregationInstance distributed() {
        return new SpreadInstance(getSize(), getExtent());
    }
}
