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
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;
import com.spotify.heroic.query.AbsoluteDateRange;
import com.spotify.heroic.query.DateRange;

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
        private final TimeSerie timeSerie;
        private final long sampling;

        public HandleGetResults(TimeSerie timeSerie, Aggregation aggregation) {
            this.timeSerie = timeSerie;
            this.sampling = aggregation.getWidth();
        }

        @Override
        public CacheQueryResult done(Collection<CacheBackendGetResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final List<DataPoint> resultDatapoints = new ArrayList<DataPoint>();
            final List<TimeSerieSlice> misses = new ArrayList<TimeSerieSlice>();

            for (final CacheBackendGetResult result : results) {
                final DataPoint[] datapoints = result.getDatapoints();

                for (int i = 0; i < datapoints.length; i++) {
                    final DataPoint d = datapoints[i];

                    if (d == null) {
                        misses.add(timeSerie.slice(rangeForDataPoint(d)));
                        continue;
                    }

                    resultDatapoints.add(d);
                }
            }

            Collections.sort(resultDatapoints);

            return new CacheQueryResult(timeSerie.getTags(), resultDatapoints,
                    TimeSerieSlice.joinAll(misses));
        }

        private DateRange rangeForDataPoint(DataPoint d) {
            final long start = d.getTimestamp();
            final long end = start + sampling;
            return new AbsoluteDateRange(start, end);
        }
    }

    public Callback<CacheQueryResult> query(TimeSerieSlice slice,
            final Aggregation aggregation) {
        final List<Long> range = calculateRange(slice, aggregation);
        final Set<CacheKey> keys = new HashSet<CacheKey>();

        for (final long base : range) {
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

        final TimeSerie timeSerie = slice.getTimeSerie();
        return callback.reduce(queries, queryTimer, new HandleGetResults(
                timeSerie, aggregation));
    }

    public Callback<Void> put(TimeSerie timeSerie, Aggregation aggregation,
            List<DataPoint> datapoints) {
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

        final Callback<Void> callback = new ConcurrentCallback<Void>();

        return callback.reduce(queries, queryTimer,
                new Callback.Reducer<CacheBackendPutResult, Void>() {
                    @Override
                    public Void done(Collection<CacheBackendPutResult> results,
                            Collection<Throwable> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        return null;
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
            final long base = calculateBucket(sampling, d.getTimestamp());
            final CacheKey key = new CacheKey(timeSerie, aggregation, base);

            DataPoint[] group = requests.get(key);

            if (group == null) {
                group = new DataPoint[WIDTH];
                requests.put(key, group);
            }

            int offset = calculateOffset(sampling, d);
            group[offset] = d;
        }

        return requests;
    }

    private List<Long> calculateRange(TimeSerieSlice slice,
            Aggregation aggregation) {
        final long period = aggregation.getWidth();
        final long first = calculateBucket(period, slice.getStart());
        final long last = calculateEndBucket(period, slice.getEnd());

        final List<Long> range = new ArrayList<Long>();

        for (long current = first; current < last; current += period) {
            range.add(current);
        }

        return range;
    }

    private int calculateOffset(final long sampling, DataPoint datapoint) {
        final long width = sampling * WIDTH;
        final long timestamp = datapoint.getTimestamp() % width;

        if (timestamp % sampling != 0)
            throw new RuntimeException(
                    "Datapoint timestamp is not a multiple of the sampling period");

        final int offset = (int) (timestamp / sampling);
        return offset;
    }

    private long calculateBucket(final long sampling, final long start) {
        final long width = sampling * WIDTH;
        return start - start % width;
    }

    private long calculateEndBucket(final long sampling, final long end) {
        final long width = sampling * WIDTH;
        return end + width - end % width;
    }
}
