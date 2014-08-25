package com.spotify.heroic.cache.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.Series;

@ToString(of = { "series", "aggregationGroup" })
@EqualsAndHashCode(of = { "series", "aggregationGroup" })
public class CacheBackendKey {
    /**
     * Includes key and tags.
     */
    @Getter
    private final Series series;

    /**
     * Always includes sampling.
     */
    @Getter
    private final AggregationGroup aggregationGroup;

    public CacheBackendKey(Series series, AggregationGroup aggregation) {
        this.series = series;
        this.aggregationGroup = aggregation;
    }
}