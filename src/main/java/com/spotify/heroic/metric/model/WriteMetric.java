package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;

@Data
public final class WriteMetric {
    private final Series series;
    private final List<DataPoint> data;

    @JsonCreator
    public static WriteMetric create(
            @JsonProperty(value = "series", required = true) Series series,
            @JsonProperty(value = "data", required = true) List<DataPoint> data) {
        return new WriteMetric(series, data);
    }
}