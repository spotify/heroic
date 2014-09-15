package com.spotify.heroic.http.rpc3;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metrics.model.MetricGroup;

@Data
public final class Rpc3MetricGroups {
    private final List<MetricGroup> groups;
    private final Rpc3Statistics statistics;

    @JsonCreator
    public static Rpc3MetricGroups create(
            @JsonProperty(value = "groups", required = true) List<MetricGroup> groups,
            @JsonProperty(value = "statistics", required = true) Rpc3Statistics statistics) {
        return new Rpc3MetricGroups(groups, statistics);
    }
}