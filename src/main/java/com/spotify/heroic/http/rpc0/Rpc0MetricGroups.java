package com.spotify.heroic.http.rpc0;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metrics.model.Statistics;

@Data
public final class Rpc0MetricGroups {
    private final List<Rpc0MetricGroup> groups;
    private final Statistics statistics;

    @JsonCreator
    public static Rpc0MetricGroups create(
            @JsonProperty(value = "groups", required = true) List<Rpc0MetricGroup> groups,
            @JsonProperty(value = "statistics", required = true) Statistics statistics) {
        return new Rpc0MetricGroups(groups, statistics);
    }
}