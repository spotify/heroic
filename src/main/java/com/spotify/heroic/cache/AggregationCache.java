package com.spotify.heroic.cache;

import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cache.model.AggregationCacheResult;
import com.spotify.heroic.model.TimeSerieSlice;
import com.spotify.heroic.yaml.ValidationException;

/**
 * Is used to query for pre-aggregated cached time series.
 * 
 * @author udoprog
 */
public interface AggregationCache {
    public static interface YAML {
        AggregationCache build(String context) throws ValidationException;
    }

    public Callback<AggregationCacheResult> query(TimeSerieSlice slice,
            Aggregation aggregation);
}
