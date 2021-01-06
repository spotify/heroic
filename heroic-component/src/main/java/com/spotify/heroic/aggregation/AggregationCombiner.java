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
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.TdigestPoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface AggregationCombiner {
    ImmutableList<MetricType> TDIGEST_TYPE = ImmutableList.of(MetricType.TDIGEST_POINT,
        MetricType.DISTRIBUTION_POINTS);
    List<ShardedResultGroup> combine(List<List<ShardedResultGroup>> all);

    AggregationCombiner DEFAULT = new AggregationCombiner() {
        @Override
        public List<ShardedResultGroup> combine(
            final List<List<ShardedResultGroup>> all
        ) {
            final ImmutableList.Builder<ShardedResultGroup> combined = ImmutableList.builder();

            for (final List<ShardedResultGroup> groups : all) {
                combined.addAll(groups);
            }

            return combined.build();
        }

        @Override
        public String toString() {
            return "DEFAULT";
        }
    };

    default void compute(final ImmutableList.Builder<ShardedResultGroup> groups,
                          final AggregationOutput out,
                          final long cadence) {
        final List<TdigestPoint> metrics = out.getMetrics().getDataAs(TdigestPoint.class);
        final Map<ComputeDistributionStat.Percentile, List<Point>> resMap = new HashMap<>();
        for (TdigestPoint tigestPoint : metrics) {
            ComputeDistributionStat
                .Percentile
                .DEFAULT
                .forEach(p -> compute(tigestPoint, resMap, p));
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
        tags.put("tdigestat", percentile.getName());
        Series newSeries = Series.of(s.getKey(), tags, s.getResource());
        newSet.add(newSeries);
    }

    private void compute(final TdigestPoint tigestPoint,
                               final Map<ComputeDistributionStat.Percentile, List<Point>> resMap,
                               final ComputeDistributionStat.Percentile percentile) {
        Point point = ComputeDistributionStat.computePercentile(tigestPoint, percentile);
        List<Point> points = resMap.getOrDefault(percentile, new ArrayList<>());
        points.add(point);
        resMap.put(percentile, points);
    }
}
