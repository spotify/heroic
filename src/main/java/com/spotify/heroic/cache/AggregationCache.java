package com.spotify.heroic.cache;

import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.TimeSerieSlice;
import com.spotify.heroic.cache.model.AggregationCacheResult;

/**
 * Is used to query for pre-aggregated cached time series.
 * 
 * @author udoprog
 */
public interface AggregationCache {
    public Callback<AggregationCacheResult> query(TimeSerieSlice slice,
            Aggregation aggregation);
}
