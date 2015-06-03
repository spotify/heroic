package com.spotify.heroic.metric.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.heroic.model.Statistics;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;

@Slf4j
@Data
public class ShardedResultGroups {
    private static final List<ShardedResultGroup> EMPTY_GROUPS = new ArrayList<>();
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final List<ShardLatency> EMPTY_LATENCIES = ImmutableList.of();

    private static final ShardedResultGroups EMPTY = new ShardedResultGroups(new ArrayList<ShardedResultGroup>(),
            Statistics.EMPTY, EMPTY_ERRORS, EMPTY_LATENCIES);

    private final List<ShardedResultGroup> groups;
    private final Statistics statistics;
    private final List<RequestError> errors;
    private final List<ShardLatency> latencies;

    private static class SelfReducer implements Collector<ShardedResultGroups, ShardedResultGroups> {
        @Override
        public ShardedResultGroups collect(Collection<ShardedResultGroups> results) throws Exception {
            ShardedResultGroups groups = ShardedResultGroups.EMPTY;

            for (final ShardedResultGroups r : results) {
                groups = groups.merge(r);
            }

            return groups;
        }
    }

    private static final SelfReducer merger = new SelfReducer();

    public static SelfReducer merger() {
        return merger;
    }

    public ShardedResultGroups merge(ShardedResultGroups other) {
        final List<ShardedResultGroup> groups = Lists.newArrayList();
        groups.addAll(this.groups);
        groups.addAll(other.groups);

        final List<RequestError> errors = Lists.newArrayList();
        errors.addAll(this.errors);
        errors.addAll(other.errors);

        final List<ShardLatency> latencies = Lists.newArrayList();
        latencies.addAll(this.latencies);
        latencies.addAll(other.latencies);

        return new ShardedResultGroups(groups, this.statistics.merge(other.statistics), errors, latencies);
    }

    public static Transform<ResultGroups, ShardedResultGroups> toSharded(final Map<String, String> shard) {
        final long start = System.currentTimeMillis();

        return new Transform<ResultGroups, ShardedResultGroups>() {
            @Override
            public ShardedResultGroups transform(ResultGroups result) throws Exception {
                final List<ShardedResultGroup> groups = new ArrayList<>();

                for (final ResultGroup group : result.getGroups()) {
                    groups.add(new ShardedResultGroup(shard, group.getTags(), group.getValues(), group.getType()));
                }

                final long end = System.currentTimeMillis();
                final long latency = end - start;
                final ImmutableList<ShardLatency> latencies = ImmutableList.of(new ShardLatency(shard, latency));

                return new ShardedResultGroups(groups, result.getStatistics(), result.getErrors(), latencies);
            }
        };
    }

    public static ShardedResultGroups nodeError(final UUID id, final String node, final Map<String, String> tags,
            Throwable e) {
        final List<RequestError> errors = Lists.newArrayList();
        errors.add(NodeError.fromThrowable(id, node, tags, e));
        return new ShardedResultGroups(EMPTY_GROUPS, Statistics.EMPTY, errors, EMPTY_LATENCIES);
    }

    public static Transform<Throwable, ShardedResultGroups> nodeError(final UUID id, final String node,
            final Map<String, String> shard) {
        final long start = System.currentTimeMillis();

        return new Transform<Throwable, ShardedResultGroups>() {
            @Override
            public ShardedResultGroups transform(Throwable e) throws Exception {
                log.error("Encountered error in transform", e);
                final long end = System.currentTimeMillis();
                final long latency = end - start;
                final ImmutableList<ShardLatency> latencies = ImmutableList.of(new ShardLatency(shard, latency));

                final List<RequestError> errors = Lists.newArrayList();
                errors.add(NodeError.fromThrowable(id, node, shard, e));
                return new ShardedResultGroups(EMPTY_GROUPS, Statistics.EMPTY, errors, latencies);
            }
        };
    }
}