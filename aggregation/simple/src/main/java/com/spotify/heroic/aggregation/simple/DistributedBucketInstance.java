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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.aggregation.BucketAggregationInstance;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.TagValues;

public abstract class DistributedBucketInstance<B extends Bucket> extends BucketAggregationInstance<B> {
    public DistributedBucketInstance(final long size, final long extent, final Set<MetricType> input, final MetricType out) {
        super(size, extent, ImmutableSet.<MetricType>builder().addAll(input).add(MetricType.SPREAD).build(), out);
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
                final Map<String, String> tags = ImmutableMap.of();
                final Set<Series> series = ImmutableSet.of();
                final AggregationSession session = session(range, ImmutableSet.of());

                // combine all the tags.
                final Iterator<ShardedResultGroup> step1 = Iterators.concat(Iterators.transform(all.iterator(), Iterable::iterator));

                final Iterator<TagValues> step2 = Iterators.concat(Iterators.transform(step1, g -> {
                    g.getGroup().updateAggregation(session, tags, series);
                    return g.getTags().iterator();
                }));

                final List<TagValues> tagValues = TagValues.fromEntries(Iterators.concat(Iterators.transform(step2, TagValues::iterator)));

                final ImmutableList.Builder<ShardedResultGroup> groups = ImmutableList.builder();

                for (final AggregationData data : session.result().getResult()) {
                    groups.add(new ShardedResultGroup(tags, tagValues, data.getMetrics(), getSize()));
                }

                return groups.build();
            }
        };
    }
}
