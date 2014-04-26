
package com.spotify.heroic.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.model.CachePutResult;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;
import com.spotify.heroic.yaml.ValidationException;

public class InMemoryAggregationCache implements AggregationCache {
    public static final int WIDTH = 1200;

    public static class YAML implements AggregationCache.YAML {
        public static final String TYPE = "!in-memory-cache";

        @Override
        public AggregationCache build(String context)
                throws ValidationException {
            return new InMemoryAggregationCache();
        }
    }

    public Map<CacheKey, DataPoint[]> cache = new HashMap<CacheKey, DataPoint[]>();

    @Override
    public Callback<CacheQueryResult> query(TimeSerieSlice slice,
            Aggregation aggregation) {
        final List<Long> range = calculateRange(slice, aggregation);

        final List<TimeSerieSlice> misses = new ArrayList<TimeSerieSlice>();
        final List<DataPoint> result = new ArrayList<DataPoint>();

        for (final long base : range) {
            final CacheKey key = new CacheKey(slice.getTimeSerie(),
                    aggregation, base);

            final DataPoint[] datapoints = cache.get(key);

            if (datapoints == null) {
                misses.add(slice.modify(base, base + aggregation.getWidth()));
                break;
            }

            for (int i = 0; i < datapoints.length; i++) {
                final DataPoint datapoint = datapoints[i];

                if (datapoint == null) {
                    long start = base + i * aggregation.getWidth();
                    long end = start + aggregation.getWidth();
                    misses.add(slice.modify(start, end));
                    continue;
                }

                result.add(datapoint);
            }
        }

        final Callback<CacheQueryResult> callback = new ConcurrentCallback<CacheQueryResult>();

        callback.finish(new CacheQueryResult(result, TimeSerieSlice
                .joinAll(misses)));

        return callback;
    }

    @Override
    public Callback<CachePutResult> put(TimeSerie timeSerie,
            Aggregation aggregation, List<DataPoint> updates) {
        for (final DataPoint update : updates) {
            final long sampling = aggregation.getWidth();
            final long base = calculateBucket(sampling, update.getTimestamp());
            final CacheKey key = new CacheKey(timeSerie, aggregation, base);

            DataPoint[] datapoints = cache.get(key);
            
            if (datapoints == null) {
                datapoints = new DataPoint[WIDTH];
                cache.put(key, datapoints);
            }

            int offset = calculateOffset(sampling, update);
            datapoints[offset] = update;
        }

        final Callback<CachePutResult> callback = new ConcurrentCallback<CachePutResult>();

        callback.finish(new CachePutResult());

        return callback;
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