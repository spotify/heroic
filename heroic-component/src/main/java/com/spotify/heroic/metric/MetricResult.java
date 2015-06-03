package com.spotify.heroic.metric;

import lombok.Data;

import com.spotify.heroic.metric.model.ShardedResultGroups;
import com.spotify.heroic.model.DateRange;

@Data
public class MetricResult {
    private final DateRange queryRange;
    private final ShardedResultGroups metricGroups;
}