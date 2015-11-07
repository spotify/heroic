package com.spotify.heroic.aggregation;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.ShardedResultGroup;

public interface AggregationCombiner {
    public List<ShardedResultGroup> combine(List<List<ShardedResultGroup>> all);

    public static AggregationCombiner DEFAULT = new AggregationCombiner() {
        @Override
        public List<ShardedResultGroup> combine(List<List<ShardedResultGroup>> all) {
            final ImmutableList.Builder<ShardedResultGroup> combined = ImmutableList.builder();

            for (final List<ShardedResultGroup> groups : all) {
                combined.addAll(groups);
            }

            return combined.build();
        }
    };
}