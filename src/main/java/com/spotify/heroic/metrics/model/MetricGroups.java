package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Data;

@Data
public final class MetricGroups {
    private final List<MetricGroup> groups;
    private final Statistics statistics;
}