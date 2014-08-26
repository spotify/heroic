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
            @JsonProperty(value = "timeSerie", required = true) Series timeSeries,
            @JsonProperty(value = "datapoints", required = true) List<DataPoint> datapoints) {
        final Series s;

        if (series != null) {
            s = series;
        } else if (timeSeries != null) {
            s = timeSeries;
        } else {
            throw new IllegalArgumentException(
                    "Neither series nor timeSeries was specified");
        }

        return new MetricGroup(s, datapoints);
    }
}