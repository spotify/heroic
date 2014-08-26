package com.spotify.heroic.http.rpc1;

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
public class Rpc1QueryBody {
    private final Series key;
    private final Set<Series> series;
    private final DateRange range;
    private final AggregationGroup aggregationGroup;

    @JsonCreator
    public static Rpc1QueryBody create(
            @JsonProperty(value = "key") Series key,
            @JsonProperty(value = "series") Set<Series> series,
            @JsonProperty(value = "range") DateRange range,
            @JsonProperty(value = "aggregationGroup") AggregationGroup aggregationGroup) {
        return new Rpc1QueryBody(key, series, range, aggregationGroup);
    }
}
