package com.spotify.heroic.cluster.httprpc.rpc5;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DateRange;

/**
 * @author udoprog
 */
@Data
public class Rpc5FullQueryBody {
    private final String backendGroup;
    private final Filter filter;
    private final List<String> groupBy;
    private final DateRange range;
    private final AggregationGroup aggregation;

    @JsonCreator
    public static Rpc5FullQueryBody create(@JsonProperty("backendGroup") String backendGroup,
            @JsonProperty("filter") Filter filter, @JsonProperty("groupBy") List<String> groupBy,
            @JsonProperty("range") DateRange range, @JsonProperty("aggregation") AggregationGroup aggregation) {
        return new Rpc5FullQueryBody(backendGroup, filter, groupBy, range, aggregation);
    }
}
