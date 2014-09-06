package com.spotify.heroic.metrics.model;

import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.model.Series;

@Data
public class GroupedSeries {
    private final Map<String, String> group;
    private final Backend backend;
    private final Set<Series> series;
    private final boolean noCache;
}