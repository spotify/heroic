package com.spotify.heroic.cache;

import java.util.List;

import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cache.model.CachePutResult;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
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

    public Callback<CacheQueryResult> query(TimeSerieSlice slice,
            Aggregation aggregation);
    
    public Callback<CachePutResult> put(TimeSerie timeSerie,
            Aggregation aggregation, List<DataPoint> datapoints);
}
