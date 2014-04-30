package com.spotify.heroic.cache;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.MetricRegistry;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.FinishedCallback;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.yaml.ValidationException;

/**
 * A reference aggregation cache implementation to allow for easier testing of application logic.
 *
 * @author udoprog
 */
public class InMemoryAggregationCacheBackend implements AggregationCacheBackend {
    public static class YAML implements AggregationCacheBackend.YAML {
        public static final String TYPE = "!in-memory-cache";

        @Override
        public AggregationCacheBackend build(String context,
                MetricRegistry registry) throws ValidationException {
            return new InMemoryAggregationCacheBackend();
        }
    }

    public Map<CacheKey, DataPoint[]> cache = new HashMap<CacheKey, DataPoint[]>();

    @Override
    public Callback<CacheBackendGetResult> get(CacheKey key)
            throws AggregationCacheException {
        DataPoint[] entry = cache.get(key);

        if (entry == null) {
            entry = new DataPoint[AggregationCache.WIDTH];
            cache.put(key, entry);
        }

        return new FinishedCallback<CacheBackendGetResult>(
                new CacheBackendGetResult(key, entry));
    }

    @Override
    public Callback<CacheBackendPutResult> put(CacheKey key,
            DataPoint[] datapoints) throws AggregationCacheException {

        DataPoint[] entry = cache.get(key);

        if (entry == null) {
            entry = new DataPoint[AggregationCache.WIDTH];
            cache.put(key, entry);
        }

        for (int i = 0; i < datapoints.length; i++) {
            if (entry[i] != null || datapoints[i] == null)
                continue;

            entry[i] = datapoints[i];
        }

        return new FinishedCallback<CacheBackendPutResult>(
                new CacheBackendPutResult());
    }
}