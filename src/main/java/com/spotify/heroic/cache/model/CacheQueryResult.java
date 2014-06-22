package com.spotify.heroic.cache.model;

import java.util.List;

import lombok.Data;
import lombok.Getter;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerieSlice;

@Data
public class CacheQueryResult {
    private final TimeSerieSlice slice;
    private final AggregationGroup aggregation;

    /**
     * Collected results so far. Should be joined by the result from the above
     * cache misses.
     */
    private final List<DataPoint> result;

    /**
     * Cache misses that has to be queried and aggregated from raw storage.
     */
    @Getter
    private final List<TimeSerieSlice> misses;
}
