package com.spotify.heroic.metric.model;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ErrorTransformer;
import com.spotify.heroic.async.Reducer;
import com.spotify.heroic.async.Transform;
import com.spotify.heroic.model.Statistics;

@Slf4j
@Data
public class ShardedMetricGroups {
    private static final List<ShardedMetricGroup> EMPTY_GROUPS = new ArrayList<>();
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();

    private static final ShardedMetricGroups EMPTY = new ShardedMetricGroups(new ArrayList<ShardedMetricGroup>(),
            Statistics.EMPTY, EMPTY_ERRORS);

    private final List<ShardedMetricGroup> groups;
    private final Statistics statistics;
    private final List<RequestError> errors;

    @JsonCreator
    public static ShardedMetricGroups create(@JsonProperty("groups") List<ShardedMetricGroup> groups,
            @JsonProperty("statistics") Statistics statistics, @JsonProperty("errors") List<RequestError> errors) {
        return new ShardedMetricGroups(groups, statistics, errors);
    }

    private static class SelfReducer implements Reducer<ShardedMetricGroups, ShardedMetricGroups> {
        @Override
        public ShardedMetricGroups resolved(Collection<ShardedMetricGroups> results, Collection<CancelReason> cancelled)
                throws Exception {
            ShardedMetricGroups groups = ShardedMetricGroups.EMPTY;

            for (final ShardedMetricGroups r : results) {
                groups = groups.merge(r);
            }

            return groups;
        }
    }

    private static final SelfReducer merger = new SelfReducer();

    public static SelfReducer merger() {
        return merger;
    }

    public ShardedMetricGroups merge(ShardedMetricGroups other) {
        final List<ShardedMetricGroup> groups = Lists.newArrayList();
        groups.addAll(this.groups);
        groups.addAll(other.groups);

        final List<RequestError> errors = Lists.newArrayList();
        errors.addAll(this.errors);
        errors.addAll(other.errors);

        return new ShardedMetricGroups(groups, this.statistics.merge(other.statistics), errors);
    }

    public static Transform<MetricGroups, ShardedMetricGroups> toSharded(final Map<String, String> shard) {
        return new Transform<MetricGroups, ShardedMetricGroups>() {
            @Override
            public ShardedMetricGroups transform(MetricGroups result) throws Exception {
                final List<ShardedMetricGroup> groups = new ArrayList<>();

                for (final MetricGroup group : result.getGroups()) {
                    groups.add(new ShardedMetricGroup(shard, group.getGroup(), group.getDatapoints()));
                }

                return new ShardedMetricGroups(groups, result.getStatistics(), result.getErrors());
            }
        };
    }

    public static ShardedMetricGroups nodeError(final UUID id, final URI uri, final Map<String, String> tags,
            Exception e) {
        final List<RequestError> errors = Lists.newArrayList();
        errors.add(NodeError.fromException(id, uri, tags, e));
        return new ShardedMetricGroups(EMPTY_GROUPS, Statistics.EMPTY, errors);
    }

    public static ErrorTransformer<ShardedMetricGroups> nodeError(final UUID id, final URI uri,
            final Map<String, String> shard) {
        return new ErrorTransformer<ShardedMetricGroups>() {
            @Override
            public ShardedMetricGroups transform(Exception e) throws Exception {
                log.error("Encountered error in transform", e);
                return ShardedMetricGroups.nodeError(id, uri, shard, e);
            }
        };
    }
}
