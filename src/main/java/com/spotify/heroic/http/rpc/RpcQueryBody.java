package com.spotify.heroic.http.rpc;

import java.util.Set;

import lombok.Data;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@Data
@ToString(of = { "key", "range", "aggregationGroup" })
public class RpcQueryBody {
    private final Series key;
    private final Set<Series> timeseries;
    private final DateRange range;
    private final AggregationGroup aggregationGroup;

    @JsonCreator
    public static RpcQueryBody create(
            @JsonProperty(value = "key", required = true) Series key,
            @JsonProperty(value = "timeseries", required = true) Set<Series> timeseries,
            @JsonProperty(value = "range", required = true) DateRange range,
            @JsonProperty(value = "aggregationGroup", required = true) AggregationGroup aggregationGroup) {
        return new RpcQueryBody(key, timeseries, range, aggregationGroup);
    }
}
