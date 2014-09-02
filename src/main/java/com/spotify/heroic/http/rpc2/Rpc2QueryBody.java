package com.spotify.heroic.http.rpc2;

import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

/**
 * @author udoprog
 */
@Data
public class Rpc2QueryBody {
    private final String backendGroup;
    private final Series key;
    private final Set<Series> series;
    private final DateRange range;
    private final AggregationGroup aggregationGroup;

    @JsonCreator
    public static Rpc2QueryBody create(
            @JsonProperty(value = "backendGroup") String backendGroup,
            @JsonProperty(value = "key") Series key,
            @JsonProperty(value = "series") Set<Series> series,
            @JsonProperty(value = "range") DateRange range,
            @JsonProperty(value = "aggregationGroup") AggregationGroup aggregationGroup) {
        return new Rpc2QueryBody(backendGroup, key, series, range,
                aggregationGroup);
    }
}
