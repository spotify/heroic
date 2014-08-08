package com.spotify.heroic.http.rpc.model;

import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

@Data
public class RpcQueryRequest {
    private final TimeSerie key;
    private final Set<TimeSerie> timeseries;
    private final DateRange range;
    private final AggregationGroup aggregationGroup;

    @JsonCreator
    public static RpcQueryRequest create(
            @JsonProperty(value = "key", required = true) TimeSerie key,
            @JsonProperty(value = "timeseries", required = true) Set<TimeSerie> timeseries,
            @JsonProperty(value = "range", required = true) DateRange range,
            @JsonProperty(value = "aggregationGroup", required = true) AggregationGroup aggregationGroup)
    {
        return new RpcQueryRequest(key, timeseries, range, aggregationGroup);
    }
}
