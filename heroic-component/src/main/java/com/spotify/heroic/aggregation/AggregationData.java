package com.spotify.heroic.aggregation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.spotify.heroic.model.MetricType;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;

@Data
public final class AggregationData {
    private final Map<String, String> group;
    private final Set<Series> series;
    private final List<? extends TimeData> values;
    private final MetricType type;
}