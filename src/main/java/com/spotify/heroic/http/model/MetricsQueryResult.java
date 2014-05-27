package com.spotify.heroic.http.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.backend.BackendManager.MetricGroups;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class MetricsQueryResult {
    @Getter
    private final DateRange queryRange;
    @Getter
    private final MetricGroups metricGroups;
}
