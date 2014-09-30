package com.spotify.heroic.metric.model;

import lombok.Data;

import com.spotify.heroic.model.DateRange;

@Data
public class QueryMetricsResult {
    private final DateRange queryRange;
    private final MetricGroups metricGroups;
}
