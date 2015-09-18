package com.spotify.heroic.aggregation;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricCollection;

import lombok.Data;

@Data
public final class AggregationData {
    private final Map<String, String> group;
    private final Set<Series> series;
    private final MetricCollection metrics;

    public static AggregationData forSeries(Series s, MetricCollection metrics) {
        return new AggregationData(s.getTags(), ImmutableSet.of(s), metrics);
    }

    public boolean isEmpty() {
        return metrics.isEmpty();
    }
}