package com.spotify.heroic.metric.model;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class MetricGroups {
    private static final List<MetricGroup> EMPTY_GROUPS = new ArrayList<>();
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();

    private static final MetricGroups EMPTY = new MetricGroups(new ArrayList<MetricGroup>(), Statistics.EMPTY,
            EMPTY_ERRORS);

    private final List<MetricGroup> groups;
    private final Statistics statistics;
    private final List<RequestError> errors;

    @JsonCreator
    public static MetricGroups create(@JsonProperty(value = "groups", required = true) List<MetricGroup> groups,
            @JsonProperty(value = "statistics", required = true) Statistics statistics,
            @JsonProperty(value = "errors", required = false) List<RequestError> errors) {
        if (errors == null)
            errors = EMPTY_ERRORS;

        return new MetricGroups(groups, statistics, errors);
    }

    private static class SelfReducer implements Reducer<MetricGroups, MetricGroups> {
        @Override
        public MetricGroups resolved(Collection<MetricGroups> results, Collection<CancelReason> cancelled)
                throws Exception {
            MetricGroups groups = MetricGroups.EMPTY;

            for (final MetricGroups r : results) {
                groups = groups.merge(r);
            }

            return groups;
        }
    }

    private static final SelfReducer merger = new SelfReducer();

    public static SelfReducer merger() {
        return merger;
    }

    public MetricGroups merge(MetricGroups other) {
        final List<MetricGroup> groups = Lists.newArrayList();
        groups.addAll(this.groups);
        groups.addAll(other.groups);

        final List<RequestError> errors = Lists.newArrayList();
        errors.addAll(this.errors);
        errors.addAll(other.errors);

        return new MetricGroups(groups, this.statistics.merge(other.statistics), errors);
    }

    public static final Transform<MetricGroups, MetricGroups> identity = new Transform<MetricGroups, MetricGroups>() {
        @Override
        public MetricGroups transform(MetricGroups result) throws Exception {
            return result;
        }
    };

    public static Transform<MetricGroups, MetricGroups> identity() {
        return identity;
    }

    public static MetricGroups nodeError(final UUID id, final URI uri, final Map<String, String> tags, Exception e) {
        final List<RequestError> errors = Lists.newArrayList();
        errors.add(NodeError.fromException(id, uri, tags, e));
        return new MetricGroups(EMPTY_GROUPS, Statistics.EMPTY, errors);
    }

    public static ErrorTransformer<MetricGroups> nodeError(final UUID id, final URI uri, final Map<String, String> shard) {
        return new ErrorTransformer<MetricGroups>() {
            @Override
            public MetricGroups transform(Exception e) throws Exception {
                return MetricGroups.nodeError(id, uri, shard, e);
            }
        };
    }

    public static MetricGroups seriesError(final Map<String, String> tags, Exception e) {
        final List<RequestError> errors = Lists.newArrayList();
        errors.add(SeriesError.fromException(tags, e));
        return new MetricGroups(EMPTY_GROUPS, Statistics.EMPTY, errors);
    }

    public static ErrorTransformer<MetricGroups> seriesError(final Map<String, String> shard) {
        return new ErrorTransformer<MetricGroups>() {
            @Override
            public MetricGroups transform(Exception e) throws Exception {
                log.error("Encountered error in transform", e);
                return MetricGroups.seriesError(shard, e);
            }
        };
    }

    public static MetricGroups fromResult(List<MetricGroup> groups, Statistics statistics) {
        return new MetricGroups(groups, statistics, EMPTY_ERRORS);
    }

    public static MetricGroups build(List<MetricGroup> groups, Statistics statistics, List<RequestError> errors) {
        if (errors == null)
            throw new NullPointerException();

        return new MetricGroups(groups, statistics, errors);
    }
}