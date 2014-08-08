package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public final class MetricGroups {
    private final List<MetricGroup> groups;
    private final Statistics statistics;

    @JsonCreator
    public static MetricGroups create(
            @JsonProperty(value = "groups", required = true) List<MetricGroup> groups,
            @JsonProperty(value = "statistics", required = true) Statistics statistics) {
        return new MetricGroups(groups, statistics);
    }
}