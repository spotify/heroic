package com.spotify.heroic.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.cache.model.CachePutResult;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@Slf4j
public class AggregationCache {
    public static final int WIDTH = 1200;

    private final AggregationCacheBackend backend;
    private final Timer getTimer;
    private final Timer putTimer;

    public AggregationCache(MetricRegistry registry,
            AggregationCacheBackend backend) {
        this.backend = backend;
        this.getTimer = registry.timer("aggregation-cache.get");
        this.putTimer = registry.timer("aggregation-cache.put");
    }

    private static final class HandleGetResults implements
            Callback.Reducer<CacheBackendGetResult, CacheQueryResult> {
        private final TimeSerieSlice slice;
        private final Aggregation aggregation;

        public HandleGetResults(TimeSerieSlice slice, Aggregation aggregation) {
            this.slice = slice;
            this.aggregation = aggregation;
        }

        @Override
        public CacheQueryResult done(Collection<CacheBackendGetResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final List<DataPoint> datapoints = new ArrayList<DataPoint>();
            final List<TimeSerieSlice> allMisses = new ArrayList<TimeSerieSlice>();

            for (final CacheBackendGetResult result : results) {
                mergeResult(datapoints, allMisses, result);
            }

            Collections.sort(datapoints);

            final List<TimeSerieSlice> misses = TimeSerieSlice
                    .joinAll(allMisses);

            for (final TimeSerieSlice miss : misses) {
                log.info("Missing: " + miss);
            }

            return new CacheQueryResult(slice, aggregation,
                    datapoints, misses);
        }

        private void mergeResult(final List<DataPoint> datapoints,
                final List<TimeSerieSlice> allMisses,
                final CacheBackendGetResult result) {
            final CacheKey key = result.getCacheKey();

            final long base = key.getBase();
            final long sampling = aggregation.getWidth();

            int first = makeIndex(sampling, slice.getStart());
            int last = makeIndex(sampling, slice.getEnd());

            final DataPoint[] source = result.getDatapoints();

            if (first > last || last >= source.length) {
                allMisses.add(slice);
                return;
            }

            for (int i = first; i <= last; i++) {
                final DataPoint d = source[i];

                if (d != null) {
                    datapoints.add(d);
                    continue;
                }

                long f = makeTimestamp(base, sampling, i);
                long t = makeTimestamp(base, sampling, i + 1);

                allMisses.add(slice.modify(f, t));
            }
        }
    }

    private static final class HandlePutResults implements
            Callback.Reducer<CacheBackendPutResult, CachePutResult> {
        @Override
        public CachePutResult done(Collection<CacheBackendPutResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            return new CachePutResult();
        }
    }

    public Callback<CacheQueryResult> get(TimeSerieSlice slice,
            final Aggregation aggregation) {
        final Set<CacheKey> keys = buildCacheKeys(slice, aggregation);
        return ConcurrentCallback.newReduce(buildBackendGetQueries(keys),
                getTimer, new HandleGetResults(slice, aggregation));
    }

    public Callback<CachePutResult> put(TimeSerie timeSerie,
            Aggregation aggregation, List<DataPoint> datapoints) {
        final Map<CacheKey, DataPoint[]> requests = buildPutRequests(timeSerie,
                aggregation, datapoints);
        return ConcurrentCallback.newReduce(buildBackendPutQueries(requests),
                putTimer, new HandlePutResults());
    }

    private Set<CacheKey> buildCacheKeys(TimeSerieSlice slice,
            final Aggregation aggregation) {
        final List<Long> bases = calculateBases(slice, aggregation);
        final Set<CacheKey> keys = new HashSet<CacheKey>();

        for (final long base : bases) {
            keys.add(new CacheKey(slice.getTimeSerie(), aggregation, base));
        }

        return keys;
    }

    private List<Callback<CacheBackendGetResult>> buildBackendGetQueries(
            final Set<CacheKey> keys) {
        final List<Callback<CacheBackendGetResult>> queries = new ArrayList<Callback<CacheBackendGetResult>>(
                keys.size());

        for (final CacheKey key : keys) {
            try {
                queries.add(backend.get(key));
            } catch (AggregationCacheException e) {
                log.error("Failed to prepare get request for cache backend", e);
            }
        }

        return queries;
    }

    private List<Callback<CacheBackendPutResult>> buildBackendPutQueries(
            final Map<CacheKey, DataPoint[]> requests) {
        final List<Callback<CacheBackendPutResult>> queries = new ArrayList<Callback<CacheBackendPutResult>>(
                requests.size());

        for (final Map.Entry<CacheKey, DataPoint[]> entry : requests.entrySet()) {
            try {
                queries.add(backend.put(entry.getKey(), entry.getValue()));
            } catch (AggregationCacheException e) {
                log.error("Failed to prepare put request for cache backend", e);
            }
        }

        return queries;
    }

    /**
     * Calculate and build all necessary put requests to update the specified
     * list of datapoints.
     * 
     * @param timeSerie
     * @param aggregation
     * @param datapoints
     * @return A map from cache keys to an array of datapoints.
     */
    private Map<CacheKey, DataPoint[]> buildPutRequests(TimeSerie timeSerie,
            Aggregation aggregation, List<DataPoint> datapoints) {
        final Map<CacheKey, DataPoint[]> requests = new HashMap<CacheKey, DataPoint[]>();

        for (final DataPoint d : datapoints) {
            final long sampling = aggregation.getWidth();
            final long base = calculateBucket(sampling, d.getTimestamp());
            final CacheKey key = new CacheKey(timeSerie, aggregation, base);

            DataPoint[] group = requests.get(key);

            if (group == null) {
                group = new DataPoint[WIDTH];
                requests.put(key, group);
            }

            int offset = makeIndex(sampling, d.getTimestamp());
            group[offset] = d;
        }

        return requests;
    }

    public static int makeIndex(long sampling, long timestamp) {
        final long relative = timestamp % (sampling * WIDTH);
        final int offset = (int) (relative / sampling);
        return offset;
    }

    public static long makeTimestamp(long base, long sampling, int index) {
        return base + sampling * index;
    }

    public static List<Long> calculateBases(TimeSerieSlice slice,
            Aggregation aggregation) {
        final long sampling = aggregation.getWidth();
        final long width = sampling * WIDTH;

        final long first = calculateBucket(sampling, slice.getStart());
        final long last = calculateBucket(sampling, slice.getEnd() + width);

        final List<Long> bases = new ArrayList<Long>();

        for (long current = first; current < last; current += width) {
            bases.add(current);
        }

        return bases;
    }

    public static long calculateBucket(long sampling, long start) {
        final long width = sampling * WIDTH;
        return start - start % width;
    }
}
