package com.spotify.heroic.model;

import static com.google.common.base.Preconditions.checkNotNull;
import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.Filter;

@Data
public class RangeFilter {
    private final Filter filter;
    private final DateRange range;
    private final int limit;

    @JsonCreator
    public RangeFilter(@JsonProperty("filter") Filter filter, @JsonProperty("range") DateRange range,
            @JsonProperty("limit") int limit) {
        this.filter = checkNotNull(filter);
        this.range = checkNotNull(range);
        this.limit = checkNotNull(limit);
    }

    public static RangeFilter filterFor(Filter filter, DateRange range) {
        return new RangeFilter(filter, range, Integer.MAX_VALUE);
    }

    public static RangeFilter filterFor(Filter filter, DateRange range, int limit) {
        return new RangeFilter(filter, range, limit);
    }
}