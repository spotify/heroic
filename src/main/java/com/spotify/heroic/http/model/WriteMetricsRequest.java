package com.spotify.heroic.http.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

@Data
public class WriteMetricsRequest {
    private final TimeSerie timeSerie;
    private final List<DataPoint> datapoints;

    @JsonCreator
    public static WriteMetricsRequest create(
            @JsonProperty("timeSerie") TimeSerie timeSerie,
            @JsonProperty("datapoints") List<DataPoint> datapoints) {
        return new WriteMetricsRequest(timeSerie, datapoints);
    }
}
