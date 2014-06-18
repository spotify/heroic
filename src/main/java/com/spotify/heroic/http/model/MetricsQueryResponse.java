package com.spotify.heroic.http.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class MetricsQueryResponse {
    @Getter
    private final DateRange queryRange;
    @Getter
    private final MetricGroups metricGroups;
}
