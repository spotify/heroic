package com.spotify.heroic.metric.model;

import java.util.List;
import java.util.Map;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;

@Data
public final class MetricGroup {
    private final Map<String, String> group;
    private final List<DataPoint> datapoints;

    @JsonCreator
    public static MetricGroup create(@JsonProperty(value = "group", required = true) Map<String, String> group,
            @JsonProperty(value = "datapoints", required = true) List<DataPoint> datapoints) {
        return new MetricGroup(group, datapoints);
    }
}