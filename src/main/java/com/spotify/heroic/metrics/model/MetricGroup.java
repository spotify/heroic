package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

@Data
public final class MetricGroup {
    private final TimeSerie timeSerie;
    private final List<DataPoint> datapoints;

    @JsonCreator
    public static MetricGroup create(
            @JsonProperty(value = "timeSerie", required = true) TimeSerie timeSerie,
            @JsonProperty(value = "datapoints", required = true) List<DataPoint> datapoints) {
        return new MetricGroup(timeSerie, datapoints);
    }
}