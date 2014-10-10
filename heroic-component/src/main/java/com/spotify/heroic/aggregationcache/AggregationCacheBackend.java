package com.spotify.heroic.aggregationcache;

import java.util.List;

import com.spotify.heroic.aggregationcache.model.CacheBackendGetResult;
import com.spotify.heroic.aggregationcache.model.CacheBackendKey;
import com.spotify.heroic.aggregationcache.model.CacheBackendPutResult;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

/**
 * Is used to query for pre-aggregated cached time series.
 *
 * @author udoprog
 */
public interface AggregationCacheBackend extends LifeCycle {
    /**
     * Get an entry from the cache.
     *
     * @param key
     *            The entry key to get.
     * @return A callback that will be executed when the entry is available with the datapoints contained in the entry.
     *         This array can contain null values to indicate that entries are missing.
     * @throws CacheOperationException
     */
    public Callback<CacheBackendGetResult> get(CacheBackendKey key, DateRange range) throws CacheOperationException;

    /**
     * Put a new entry into the aggregation cache.
     *
     *
     *
     * @param key
     * @param datapoints
     *            An array of datapoints, <code>null</code> entries should be ignored.
     * @return A callback that will be executed as soon as any underlying request has been satisfied.
     * @throws CacheOperationException
     *             An early throw exception, if the backend is unable to prepare the request.
     */
    public Callback<CacheBackendPutResult> put(CacheBackendKey key, List<DataPoint> datapoints)
            throws CacheOperationException;
}
