package com.spotify.heroic.http.rpc4;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metric.model.MetricGroup;
import com.spotify.heroic.metric.model.RequestError;
import com.spotify.heroic.model.Statistics;

@Data
public final class Rpc4MetricGroups {
    private final List<MetricGroup> groups;
    private final Statistics statistics;
    private final List<RequestError> errors;

    @JsonCreator
    public static Rpc4MetricGroups create(@JsonProperty(value = "groups", required = true) List<MetricGroup> groups,
            @JsonProperty(value = "statistics", required = true) Statistics statistics,
            @JsonProperty(value = "errors", required = true) List<RequestError> errors) {
        return new Rpc4MetricGroups(groups, statistics, errors);
    }
}