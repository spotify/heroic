package com.spotify.heroic.metrics.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;

@Data
public final class MetricGroups {
    private static final MetricGroups EMPTY = new MetricGroups(
            new ArrayList<MetricGroup>(), Statistics.EMPTY);

    private final List<MetricGroup> groups;
    private final Statistics statistics;

    @JsonCreator
    public static MetricGroups create(
            @JsonProperty(value = "groups", required = true) List<MetricGroup> groups,
            @JsonProperty(value = "statistics", required = true) Statistics statistics) {
        return new MetricGroups(groups, statistics);
    }

    @Slf4j
    private static class Merger implements
            Callback.Reducer<MetricGroups, MetricGroups> {
        @Override
        public MetricGroups resolved(Collection<MetricGroups> results,
                Collection<Exception> errors, Collection<CancelReason> cancelled)
                throws Exception {
            for (final Exception e : errors)
                log.error("Query failed", e);

            for (final CancelReason cancel : cancelled)
                log.error("Query cancelled: {}", cancel);

            MetricGroups groups = MetricGroups.EMPTY;

            for (final MetricGroups r : results) {
                groups = groups.merge(r);
            }

            return groups;
        }
    }

    private static final Merger merger = new Merger();

    public static Merger merger() {
        return merger;
    }

    public MetricGroups merge(MetricGroups other) {
        final List<MetricGroup> groups = Lists.newArrayList();
        groups.addAll(this.groups);
        groups.addAll(other.groups);
        return new MetricGroups(groups, this.statistics.merge(other.statistics));
    }
}