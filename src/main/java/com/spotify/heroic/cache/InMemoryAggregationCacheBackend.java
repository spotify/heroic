package com.spotify.heroic.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

/**
 * A reference aggregation cache implementation to allow for easier testing of
 * application logic.
 *
 * @author udoprog
 */
public class InMemoryAggregationCacheBackend implements AggregationCacheBackend {
    public static class YAML implements AggregationCacheBackend.YAML {
        public static final String TYPE = "!in-memory-cache";

        @Override
        public AggregationCacheBackend build(String context,
                AggregationCacheBackendReporter reporter)
                throws ValidationException {
            return new InMemoryAggregationCacheBackend();
        }
    }

    public Map<CacheBackendKey, Map<Long, DataPoint>> cache = new HashMap<CacheBackendKey, Map<Long, DataPoint>>();

    @Override
    public synchronized Callback<CacheBackendGetResult> get(
            CacheBackendKey key, DateRange range)
            throws AggregationCacheException {
        Map<Long, DataPoint> entry = cache.get(key);

        if (entry == null) {
            entry = new HashMap<Long, DataPoint>();
            cache.put(key, entry);
        }

        final AggregationGroup aggregation = key.getAggregationGroup();
        final long width = aggregation.getSampling().getSize();

        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        if (width == 0) {
            return new ResolvedCallback<CacheBackendGetResult>(
                    new CacheBackendGetResult(key, datapoints));
        }

        final long start = range.getStart() - range.getStart() % width;
        final long end = range.getEnd() - range.getEnd() % width;

        for (long i = start; i < end; i += width) {
            final DataPoint d = entry.get(i);

            if (d == null)
                continue;

            datapoints.add(d);
        }

        return new ResolvedCallback<CacheBackendGetResult>(
                new CacheBackendGetResult(key, datapoints));
    }

    @Override
    public synchronized Callback<CacheBackendPutResult> put(
            CacheBackendKey key, List<DataPoint> datapoints)
            throws AggregationCacheException {
        Map<Long, DataPoint> entry = cache.get(key);

        if (entry == null) {
            entry = new HashMap<Long, DataPoint>();
            cache.put(key, entry);
        }

        final AggregationGroup aggregator = key.getAggregationGroup();
        final long width = aggregator.getSampling().getSize();

        if (width == 0) {
            return new ResolvedCallback<CacheBackendPutResult>(
                    new CacheBackendPutResult());
        }

        for (final DataPoint d : datapoints) {
            final long timestamp = d.getTimestamp();
            final double value = d.getValue();
            final float p = d.getP();

            if (Double.isNaN(value))
                continue;

            if (Float.isNaN(p))
                continue;

            if (timestamp % width != 0)
                continue;

            entry.put(timestamp, d);
        }

        return new ResolvedCallback<CacheBackendPutResult>(
                new CacheBackendPutResult());
    }
}