package com.spotify.heroic.http.model;

import lombok.Data;

import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;

@Data
public class MetricsQueryResponse {
    private final DateRange queryRange;
    private final MetricGroups metricGroups;
}
