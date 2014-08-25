package com.spotify.heroic.cache.cassandra.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.Series;

@ToString(of = { "series", "aggregationGroup", "base" })
@EqualsAndHashCode(of = { "series", "aggregationGroup", "base" })
public class CacheKey {
    public static final int VERSION = 1;

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

    /**
     * long base.
     */
    @Getter
    private final long base;

    public CacheKey(Series series, AggregationGroup aggregationGroup,
            long base) {
        this.series = series;
        this.aggregationGroup = aggregationGroup;
        this.base = base;
    }
}
