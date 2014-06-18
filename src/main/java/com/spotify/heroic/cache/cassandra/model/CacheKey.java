package com.spotify.heroic.cache.cassandra.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.TimeSerie;

@ToString(of = { "timeSerie", "aggregationGroup", "base" })
@EqualsAndHashCode(of = { "timeSerie", "aggregationGroup", "base" })
public class CacheKey {
    public static final int VERSION = 1;

    /**
     * Includes key and tags.
     */
    @Getter
    private final TimeSerie timeSerie;

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

    public CacheKey(TimeSerie timeSerie, AggregationGroup aggregationGroup, long base) {
        this.timeSerie = timeSerie;
        this.aggregationGroup = aggregationGroup;
        this.base = base;
    }
}
