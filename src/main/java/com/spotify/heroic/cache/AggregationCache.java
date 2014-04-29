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
    private final Timer queryTimer;

    public AggregationCache(MetricRegistry registry,
            AggregationCacheBackend backend) {
        this.backend = backend;
        this.queryTimer = registry.timer("aggregation-cache.query");
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
            final List<DataPoint> resultDatapoints = new ArrayList<DataPoint>();
            final List<TimeSerieSlice> misses = new ArrayList<TimeSerieSlice>();

            for (final CacheBackendGetResult result : results) {
                final DataPoint[] datapoints = result.getDatapoints();

                final CacheKey key = result.getCacheKey();
                long sampling = aggregation.getWidth();

                int first = makeIndex(sampling, slice.getStart());
                int last = makeIndex(sampling, slice.getEnd());

                for (int i = first; i < last; i++) {
                    final DataPoint d = datapoints[i];
                    log.info(i + " = " + d);

                    if (d == null) {
                        long f = makeTimestamp(key.getBase(), sampling, i);
                        long t = makeTimestamp(key.getBase(), sampling, i + 1);
                        misses.add(slice.modify(f, t));
                        continue;
                    }

                    resultDatapoints.add(d);
                }
            }

            Collections.sort(resultDatapoints);

            final List<TimeSerieSlice> allMisses = TimeSerieSlice
                    .joinAll(misses);

            return new CacheQueryResult(slice, aggregation,
                    resultDatapoints, allMisses);
        }
    }

    public Callback<CacheQueryResult> query(TimeSerieSlice slice,
            final Aggregation aggregation) {
        final List<Long> buckets = calculateBuckets(slice, aggregation);
        final Set<CacheKey> keys = new HashSet<CacheKey>();

        for (final long base : buckets) {
            keys.add(new CacheKey(slice.getTimeSerie(), aggregation, base));
        }

        final List<Callback<CacheBackendGetResult>> queries = new ArrayList<Callback<CacheBackendGetResult>>(
                keys.size());

        for (final CacheKey key : keys) {
            try {
                queries.add(backend.get(key));
            } catch (AggregationCacheException e) {
                log.error("Failed to prepare get request for cache backend", e);
            }
        }

        final Callback<CacheQueryResult> callback = new ConcurrentCallback<CacheQueryResult>();

        return callback.reduce(queries, queryTimer, new HandleGetResults(
slice,
                aggregation));
    }

    public Callback<CachePutResult> put(TimeSerie timeSerie,
            Aggregation aggregation, List<DataPoint> datapoints) {
        final Map<CacheKey, DataPoint[]> requests = buildPutRequests(timeSerie,
                aggregation, datapoints);

        final List<Callback<CacheBackendPutResult>> queries = new ArrayList<Callback<CacheBackendPutResult>>(
                requests.size());

        for (Map.Entry<CacheKey, DataPoint[]> entry : requests.entrySet()) {
            try {
                queries.add(backend.put(entry.getKey(), entry.getValue()));
            } catch (AggregationCacheException e) {
                log.error("Failed to prepare put request for cache backend", e);
            }
        }

        final Callback<CachePutResult> callback = new ConcurrentCallback<CachePutResult>();

        return callback.reduce(queries, queryTimer,
                new Callback.Reducer<CacheBackendPutResult, CachePutResult>() {
                    @Override
                    public CachePutResult done(
                            Collection<CacheBackendPutResult> results,
                            Collection<Throwable> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        return new CachePutResult();
                    }
                });
    }

    /**
     * Calculate and build all necessary put requests to update the specified
     * list of datapoints.
     * 
     * @param timeSerie
     * @param aggregation
     * @param datapoints
     * @return
     */
    private Map<CacheKey, DataPoint[]> buildPutRequests(TimeSerie timeSerie,
            Aggregation aggregation, List<DataPoint> datapoints) {
        final Map<CacheKey, DataPoint[]> requests = new HashMap<CacheKey, DataPoint[]>();

        for (final DataPoint d : datapoints) {
            final long sampling = aggregation.getWidth();
            final long width = sampling * WIDTH;
            final long base = calculateBucket(width, d.getTimestamp());
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

    private List<Long> calculateBuckets(TimeSerieSlice slice,
            Aggregation aggregation) {
        final long width = aggregation.getWidth() * WIDTH;
        final long first = calculateBucket(width, slice.getStart());
        final long last = calculateEndBucket(width, slice.getEnd());

        final List<Long> buckets = new ArrayList<Long>();

        for (long current = first; current < last; current += width) {
            buckets.add(current);
        }

        return buckets;
    }

    private static int makeIndex(long sampling, long timestamp) {
        final long relative = timestamp % (sampling * WIDTH);
        final int offset = (int) (relative / sampling);
        return offset;
    }

    private static long makeTimestamp(long base, long sampling, int index) {
        return base + sampling * index;
    }

    private static long calculateBucket(long width, long start) {
        return start - start % width;
    }

    private static long calculateEndBucket(long width, long end) {
        return end + width - end % width;
    }
}
