package com.spotify.heroic.aggregation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;

@Data
public final class AggregationData {
    private final Map<String, String> group;
    private final Set<Series> series;
    private final List<? extends Metric> values;
    private final MetricType type;
}