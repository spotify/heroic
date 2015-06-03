package com.spotify.heroic.aggregationcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.ToString;

import com.google.inject.Inject;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregationcache.model.CacheBackendGetResult;
import com.spotify.heroic.aggregationcache.model.CacheBackendKey;
import com.spotify.heroic.aggregationcache.model.CacheBackendPutResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

/**
 * A reference aggregation cache implementation to allow for easier testing of application logic.
 *
 * @author udoprog
 */
@ToString
public class InMemoryAggregationCacheBackend implements AggregationCacheBackend {
    private final Map<CacheBackendKey, Map<Long, DataPoint>> cache = new HashMap<CacheBackendKey, Map<Long, DataPoint>>();

    @Inject
    private AsyncFramework async;

    @Override
    public synchronized AsyncFuture<CacheBackendGetResult> get(CacheBackendKey key, DateRange range)
            throws CacheOperationException {
        Map<Long, DataPoint> entry = cache.get(key);

        if (entry == null) {
            entry = new HashMap<Long, DataPoint>();
            cache.put(key, entry);
        }

        final Aggregation aggregation = key.getAggregation();

        final Sampling sampling = aggregation.sampling();

        if (sampling == null)
            throw new CacheOperationException("provided aggregation is not cacheable");

        final long width = sampling.getSize();

        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        if (width == 0) {
            return async.resolved(new CacheBackendGetResult(key, datapoints));
        }

        final long start = range.getStart() - range.getStart() % width;
        final long end = range.getEnd() - range.getEnd() % width;

        for (long i = start; i < end; i += width) {
            final DataPoint d = entry.get(i);

            if (d == null)
                continue;

            datapoints.add(d);
        }

        return async.resolved(new CacheBackendGetResult(key, datapoints));
    }

    @Override
    public synchronized AsyncFuture<CacheBackendPutResult> put(CacheBackendKey key, List<DataPoint> datapoints)
            throws CacheOperationException {
        Map<Long, DataPoint> entry = cache.get(key);

        if (entry == null) {
            entry = new HashMap<Long, DataPoint>();
            cache.put(key, entry);
        }

        final Aggregation aggregation = key.getAggregation();
        final long width = aggregation.sampling().getSize();

        if (width == 0)
            return async.resolved(new CacheBackendPutResult());

        for (final DataPoint d : datapoints) {
            final long timestamp = d.getTimestamp();
            final double value = d.getValue();

            if (Double.isNaN(value))
                continue;

            if (timestamp % width != 0)
                continue;

            entry.put(timestamp, d);
        }

        return async.resolved(new CacheBackendPutResult());
    }

    @Override
    public AsyncFuture<Void> start() throws Exception {
        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
        return async.resolved(null);
    }

    @Override
    public boolean isReady() {
        return true;
    }
}