package com.spotify.heroic.aggregation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;

import lombok.Data;

@Data
public final class AggregationData {
    private final Map<String, String> group;
    private final Set<Series> series;
    private final List<? extends Metric> values;
    private final MetricType type;

    public static AggregationData forSeries(Series s, ImmutableList<? extends Metric> values, MetricType type) {
        return new AggregationData(s.getTags(), ImmutableSet.of(s), values, type);
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }
}