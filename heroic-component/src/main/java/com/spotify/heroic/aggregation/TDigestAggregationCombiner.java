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

package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.DistributionPoint;
import com.spotify.heroic.metric.HeroicDistribution;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.TdigestPoint;
import com.tdunning.math.stats.MergingDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TDigestAggregationCombiner implements AggregationCombiner {
    private final AggregationInstance reducer;
    private final DateRange range;
    private final BucketStrategy bucketStrategy;
    private final long cadence;

    private TDigestAggregationCombiner(
        final AggregationInstance reducer,
        final DateRange range,
        final BucketStrategy bucketStrategy,
        final long cadence
    ) {
        this.reducer = reducer;
        this.range = range;
        this.bucketStrategy = bucketStrategy;
        this.cadence = cadence;
    }

    public static TDigestAggregationCombiner create(
        final AggregationInstance root,
        final DateRange range,
        final BucketStrategy bucketStrategy
    ) {
        return new TDigestAggregationCombiner(root.reducer(), range, bucketStrategy,
            root.cadence());
    }

    @Override
    public List<ShardedResultGroup> combine(List<List<ShardedResultGroup>> all) {
            return distributedCombine(all);
    }

    @Override
    public String toString() {
        return "DISTRIBUTED_TDIGESGT_COMBINER";
    }

    private List<ShardedResultGroup> distributedCombine(List<List<ShardedResultGroup>> all) {
        final AggregationSession session =
            reducer.session(range, RetainQuotaWatcher.NO_QUOTA, bucketStrategy);

        for (List<ShardedResultGroup> groups : all) {
            for (final ShardedResultGroup g : groups) {
                g.getMetrics().updateAggregation(session, g.getKey(), g.getSeries());
            }
        }

        final ImmutableList.Builder<ShardedResultGroup> groups = ImmutableList.builder();

        final AggregationResult result = session.result();

        for (final AggregationOutput out : result.getResult()) {
            compute(groups, out, cadence);
        }
        return groups.build();
    }

    private void compute(final ImmutableList.Builder<ShardedResultGroup> groups,
                         final AggregationOutput out,
                         final long cadence) {
        final List<TdigestPoint> metrics = out.getMetrics().getDataAs(TdigestPoint.class);
        final Map<ComputeDistributionStat.Percentile, List<Point>> resMap = new HashMap<>();
        for (TdigestPoint tdigestPoint : metrics) {
            ComputeDistributionStat
                .Percentile
                .DEFAULT
                .forEach(p -> compute(tdigestPoint, resMap, p));
        }
        for (Map.Entry<ComputeDistributionStat.Percentile,
            List<Point>> entry : resMap.entrySet()) {
            Set<Series> newSet = new HashSet<>();
            out.getSeries().forEach(s -> updateMetadata(s, entry.getKey(), newSet));
            groups.add(new ShardedResultGroup(ImmutableMap.of(), out.getKey(), newSet,
                MetricCollection.points(entry.getValue()), cadence));
        }
    }

    private void updateMetadata(final Series s,
                                final ComputeDistributionStat.Percentile percentile,
                                final Set<Series> newSet) {
        Map<String, String> tags = new HashMap<>(s.getTags());
        tags.put("tdigeststat", percentile.getName());
        Series newSeries = Series.of(s.getKey(), tags, s.getResource());
        newSet.add(newSeries);
    }

    private void compute(final TdigestPoint tdigestPoint,
                         final Map<ComputeDistributionStat.Percentile, List<Point>> resMap,
                         final ComputeDistributionStat.Percentile percentile) {
        Point point = ComputeDistributionStat.computePercentile(tdigestPoint, percentile);
        List<Point> points = resMap.getOrDefault(percentile, new ArrayList<>());
        points.add(point);
        resMap.put(percentile, points);
    }

    public static List<ShardedResultGroup> simpleCombine(List<List<ShardedResultGroup>> all) {
        List<ShardedResultGroup> groups = AggregationCombiner.DEFAULT.combine(all);
        return computeCount(groups);
    }

    private static List<ShardedResultGroup> computeCount(List<ShardedResultGroup> groups) {
        return groups.parallelStream().map(grp -> new ShardedResultGroup(grp.getShard(),
            grp.getKey(), grp.getSeries(),
            compute(grp.getMetrics()
                .getDataAs(DistributionPoint.class)),
            grp.getCadence()))
            .collect(Collectors.toList());
    }

    private  static MetricCollection compute(List<DistributionPoint> datapoints) {
        List<Point> points = new ArrayList<>();
        for (DistributionPoint dp : datapoints) {
            if (dp.value().getValue() == null ||
                dp.value().getValue().equals(ByteString.EMPTY)) {
                points.add(new Point(dp.getTimestamp(), -1));
            } else if (dp.value() instanceof HeroicDistribution) {
                HeroicDistribution heroicDistribution = (HeroicDistribution) dp.value();
                MergingDigest mergingDigest = MergingDigest
                    .fromBytes(heroicDistribution.toByteBuffer());
                points.add(new Point(dp.getTimestamp(), mergingDigest.size()));
            }
        }
        return MetricCollection.points(points);
    }
}
