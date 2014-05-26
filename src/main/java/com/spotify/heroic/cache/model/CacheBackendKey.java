package com.spotify.heroic.cache.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.TimeSerie;


@ToString(of = { "timeSerie", "aggregationGroup" })
@EqualsAndHashCode(of = { "timeSerie", "aggregationGroup" })
public class CacheBackendKey {
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

    public CacheBackendKey(TimeSerie timeSerie, AggregationGroup aggregation) {
        this.timeSerie = timeSerie;
        this.aggregationGroup = aggregation;
    }
}