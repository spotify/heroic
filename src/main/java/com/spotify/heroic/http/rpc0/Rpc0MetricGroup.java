package com.spotify.heroic.http.rpc0;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;

@Data
public final class Rpc0MetricGroup {
    private final Series timeSerie;
    private final List<DataPoint> datapoints;

    @JsonCreator
    public static Rpc0MetricGroup create(
            @JsonProperty(value = "timeSerie", required = true) Series timeSerie,
            @JsonProperty(value = "datapoints", required = true) List<DataPoint> datapoints) {
        return new Rpc0MetricGroup(timeSerie, datapoints);
    }
}