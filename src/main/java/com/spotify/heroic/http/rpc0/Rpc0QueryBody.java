package com.spotify.heroic.http.rpc0;

import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@Data
public class Rpc0QueryBody {
    private final Series key;
    private final Set<Series> timeseries;
    private final DateRange range;
    private final AggregationGroup aggregationGroup;

    @JsonCreator
    public static Rpc0QueryBody create(
            @JsonProperty(value = "key") Series key,
            @JsonProperty(value = "timeseries") Set<Series> timeSeries,
            @JsonProperty(value = "range") DateRange range,
            @JsonProperty(value = "aggregationGroup") AggregationGroup aggregationGroup) {
        return new Rpc0QueryBody(key, timeSeries, range, aggregationGroup);
    }
}
