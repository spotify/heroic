package com.spotify.heroic.metrics.model;

import java.util.Set;

import lombok.Data;

import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.model.TimeSerie;

@Data
public class GroupedTimeSeries {
    private final TimeSerie key;
    private final MetricBackend backend;
    private final Set<TimeSerie> series;
    private final boolean noCache;
}