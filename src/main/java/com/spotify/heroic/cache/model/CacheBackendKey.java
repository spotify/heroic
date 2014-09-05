package com.spotify.heroic.cache.model;

import java.util.Map;

import lombok.Data;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.filter.Filter;

@Data
public class CacheBackendKey {
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
}