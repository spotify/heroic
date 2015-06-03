package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;

@Data
public class WriteMetric {
    private final Series series;
    private final List<DataPoint> data;

    @JsonCreator
    public WriteMetric(@JsonProperty("series") Series series, @JsonProperty("data") List<DataPoint> data) {
        this.series = series;
        this.data = data;
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }
}