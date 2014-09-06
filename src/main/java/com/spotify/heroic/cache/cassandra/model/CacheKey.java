package com.spotify.heroic.cache.cassandra.model;

import java.util.Map;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.filter.Filter;

@Data
public class CacheKey {
    public static final int VERSION = 2;

    private final int version;

    /**
     * Which filter was used to query the specified data.
     */
    private final Filter filter;

    /**
     * Which group this result belongs to.
     */
    private final Map<String, String> group;

    /**
     * Always includes sampling.
     */
    private final AggregationGroup aggregation;

    /**
     * long base.
     */
    private final long base;

    @JsonCreator
    public static CacheKey create(@JsonProperty("version") Integer version,
            @JsonProperty("filter") Filter filter,
            @JsonProperty("group") Map<String, String> group,
            @JsonProperty("aggregation") AggregationGroup aggregation,
            @JsonProperty("base") Long base) {
        return new CacheKey(version, filter, group, aggregation, base);
    }
}
