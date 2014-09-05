package com.spotify.heroic.metrics.model;

import java.util.List;
import java.util.Map;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;

@Data
public final class MetricGroup {
    private final Map<String, String> group;
    private final List<DataPoint> datapoints;

    @JsonCreator
    public static MetricGroup create(
            @JsonProperty(value = "group", required = true) Map<String, String> group,
            @JsonProperty(value = "series", required = true) Series series,
            @JsonProperty(value = "timeSerie", required = true) Series timeSeries,
            @JsonProperty(value = "datapoints", required = true) List<DataPoint> datapoints) {
        final Map<String, String> g;

        if (group != null) {
            g = group;
        } else if (series != null) {
            g = series.getTags();
        } else if (timeSeries != null) {
            g = timeSeries.getTags();
        } else {
            throw new IllegalArgumentException(
                    "Neither series nor timeSeries was specified");
        }

        return new MetricGroup(g, datapoints);
    }
}