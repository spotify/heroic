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

package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.ShardedResultGroup;
import java.util.List;
import lombok.Data;

@Data
public class DistributedAggregationCombiner implements AggregationCombiner {
    private final AggregationInstance reducer;
    private final DateRange range;
    private final BucketStrategy bucketStrategy;
    private final long cadence;

    /**
     * Create a combiner from a global aggregation.
     * <p>
     * Notice that the cadence is taken from the root aggregation, since the reducer might
     * lose it.
     *
     * @param root
     * @param range
     * @return
     */
    public static DistributedAggregationCombiner create(
        final AggregationInstance root, final DateRange range, final BucketStrategy bucketStrategy
    ) {
        return new DistributedAggregationCombiner(root.reducer(), range, bucketStrategy,
            root.cadence());
    }

    @Override
    public List<ShardedResultGroup> combine(
        final List<List<ShardedResultGroup>> all
    ) {
        final AggregationSession session =
            reducer.session(range, RetainQuotaWatcher.NO_QUOTA, bucketStrategy);

        /* iterate through all groups and setup, and feed a reducer session for every group */
        for (List<ShardedResultGroup> groups : all) {
            for (final ShardedResultGroup g : groups) {
                g.getMetrics().updateAggregation(session, g.getKey(), g.getSeries());
            }
        }

        /* build results from every reducer group into a final result */
        final ImmutableList.Builder<ShardedResultGroup> groups = ImmutableList.builder();

        final AggregationResult result = session.result();

        for (final AggregationOutput out : result.getResult()) {
            groups.add(new ShardedResultGroup(ImmutableMap.of(), out.getKey(), out.getSeries(),
                out.getMetrics(), cadence));
        }

        return groups.build();
    }
}
