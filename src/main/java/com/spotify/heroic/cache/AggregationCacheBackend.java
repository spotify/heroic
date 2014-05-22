package com.spotify.heroic.cache;

import java.util.List;

import com.codahale.metrics.MetricRegistry;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.yaml.ValidationException;

/**
 * Is used to query for pre-aggregated cached time series.
 * 
 * @author udoprog
 */
public interface AggregationCacheBackend {
    public static interface YAML {
        AggregationCacheBackend build(String context, MetricRegistry registry)
                throws ValidationException;
    }

    /**
     * Get an entry from the cache.
     * 
     * @param key
     *            The entry key to get.
     * @return A callback that will be executed when the entry is available with
     *         the datapoints contained in the entry. This array can contain
     *         null values to indicate that entries are missing.
     * @throws AggregationCacheException
     */
    public Callback<CacheBackendGetResult> get(CacheBackendKey key, DateRange range)
            throws AggregationCacheException;

    /**
     * Put a new entry into the aggregation cache.
     * 
     * 
     * 
     * @param key
     * @param datapoints
     *            An array of datapoints, <code>null</code> entries should be
     *            ignored.
     * @return A callback that will be executed as soon as any underlying
     *         request has been satisfied.
     * @throws AggregationCacheException
     *             An early throw exception, if the backend is unable to prepare
     *             the request.
     */
    public Callback<CacheBackendPutResult> put(CacheBackendKey key, List<DataPoint> datapoints) throws AggregationCacheException;
}
