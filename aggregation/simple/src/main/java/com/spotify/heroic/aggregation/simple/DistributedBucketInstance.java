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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.aggregation.BucketAggregationInstance;
import com.spotify.heroic.aggregation.BucketReducerSession;
import com.spotify.heroic.aggregation.ReducerSession;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;

import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @Override
    public AggregationCombiner combiner(final DateRange range) {
        return new AggregationCombiner() {
            @Override
            public List<ShardedResultGroup> combine(List<List<ShardedResultGroup>> all) {
                final Map<String, String> key = ImmutableMap.of();
                final ReducerSession session = reducer(range);

                final SeriesValues.Builder series = SeriesValues.builder();

                for (final List<ShardedResultGroup> groups : all) {
                    for (final ShardedResultGroup g : groups) {
                        g.getGroup().updateReducer(session, key);
                        series.addSeriesValues(g.getSeries());
                    }
                }

                final SeriesValues s = series.build();

                final ImmutableList.Builder<ShardedResultGroup> groups = ImmutableList.builder();

                for (final MetricCollection metrics : session.result().getResult()) {
                    groups.add(
                        new ShardedResultGroup(ImmutableMap.of(), key, s, metrics, getSize()));
                }

                return groups.build();
            }
        };
    }

    @Override
    public ReducerSession reducer(DateRange range) {
        return new BucketReducerSession<B>(out, size, this::buildBucket, this::build, range);
    }
}
