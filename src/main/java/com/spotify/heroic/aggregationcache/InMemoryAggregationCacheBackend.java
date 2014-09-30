package com.spotify.heroic.aggregationcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregationcache.model.CacheBackendGetResult;
import com.spotify.heroic.aggregationcache.model.CacheBackendKey;
import com.spotify.heroic.aggregationcache.model.CacheBackendPutResult;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

/**
 * A reference aggregation cache implementation to allow for easier testing of
 * application logic.
 *
 * @author udoprog
 */
public class InMemoryAggregationCacheBackend implements AggregationCacheBackend {
    private final Map<CacheBackendKey, Map<Long, DataPoint>> cache = new HashMap<CacheBackendKey, Map<Long, DataPoint>>();

    @Override
    public synchronized Callback<CacheBackendGetResult> get(
            CacheBackendKey key, DateRange range)
                    throws CacheOperationException {
        Map<Long, DataPoint> entry = cache.get(key);

        if (entry == null) {
            entry = new HashMap<Long, DataPoint>();
            cache.put(key, entry);
        }

        final AggregationGroup aggregation = key.getAggregation();
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

        return new ResolvedCallback<>(
                new CacheBackendGetResult(key, datapoints));
    }

    @Override
    public synchronized Callback<CacheBackendPutResult> put(
            CacheBackendKey key, List<DataPoint> datapoints)
                    throws CacheOperationException {
        Map<Long, DataPoint> entry = cache.get(key);

        if (entry == null) {
            entry = new HashMap<Long, DataPoint>();
            cache.put(key, entry);
        }

        final AggregationGroup aggregator = key.getAggregation();
        final long width = aggregator.getSampling().getSize();

        if (width == 0) {
            return new ResolvedCallback<CacheBackendPutResult>(
                    new CacheBackendPutResult());
        }

        for (final DataPoint d : datapoints) {
            final long timestamp = d.getTimestamp();
            final double value = d.getValue();

            if (Double.isNaN(value))
                continue;

            if (timestamp % width != 0)
                continue;

            entry.put(timestamp, d);
        }

        return new ResolvedCallback<>(new CacheBackendPutResult());
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public boolean isReady() {
        return true;
    }
}