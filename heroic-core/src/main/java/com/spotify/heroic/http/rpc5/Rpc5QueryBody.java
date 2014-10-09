package com.spotify.heroic.http.rpc5;

import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

/**
 * @author udoprog
 */
@Data
public class Rpc5QueryBody {
    private final String backendGroup;
    private final Map<String, String> group;
    private final Filter filter;
    private final Set<Series> series;
    private final DateRange range;
    private final AggregationGroup aggregationGroup;

    @JsonCreator
    public static Rpc5QueryBody create(@JsonProperty("backendGroup") String backendGroup,
            @JsonProperty("group") Map<String, String> group, @JsonProperty("filter") Filter filter,
            @JsonProperty("series") Set<Series> series, @JsonProperty("range") DateRange range,
            @JsonProperty("aggregationGroup") AggregationGroup aggregationGroup) {
        return new Rpc5QueryBody(backendGroup, group, filter, series, range, aggregationGroup);
    }
}
