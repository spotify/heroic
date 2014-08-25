package com.spotify.heroic.metrics.model;

import java.util.Set;

import lombok.Data;

import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.model.Series;

@Data
public class GroupedTimeSeries {
    private final Series key;
    private final Backend backend;
    private final Set<Series> series;
    private final boolean noCache;
}