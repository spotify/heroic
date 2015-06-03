package com.spotify.heroic.http.write;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;

@Data
public class WriteMetrics {
    private final Series series;
    private final List<DataPoint> data;

    @JsonCreator
    public static WriteMetrics create(@JsonProperty("series") Series series, @JsonProperty("data") List<DataPoint> data) {
        return new WriteMetrics(series, data);
    }
}
