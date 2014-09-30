package com.spotify.heroic.metric.model;

import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.model.Series;

@Data
public class GroupedSeries {
    private final Map<String, String> group;
    private final MetricBackend backend;
    private final Set<Series> series;
    private final boolean noCache;
}