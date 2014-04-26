package com.spotify.heroic.model;

import lombok.Getter;

import com.spotify.heroic.aggregator.Aggregation;

public class CacheKey {
    /**
     * Includes key and tags.
     */
    @Getter
    private final TimeSerie timeSerie;

    /**
     * Always includes sampling.
     */
    @Getter
    private final Aggregation aggregation;

    public CacheKey(TimeSerie timeSerie, Aggregation aggregation) {
        this.timeSerie = timeSerie;
        this.aggregation = aggregation;
    }
}
