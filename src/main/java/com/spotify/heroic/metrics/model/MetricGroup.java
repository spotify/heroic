package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;

@Data
public final class MetricGroup {
    private final Series series;
    private final List<DataPoint> datapoints;

    @JsonCreator
    public static MetricGroup create(
            @JsonProperty(value = "series", required = true) Series series,
            @JsonProperty(value = "datapoints", required = true) List<DataPoint> datapoints) {
        return new MetricGroup(series, datapoints);
    }
}