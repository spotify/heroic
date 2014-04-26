
package com.spotify.heroic.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cache.model.AggregationCacheResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerieSlice;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
public class InMemoryAggregationCache implements AggregationCache {
    public static final long WIDTH = 1200;

    public static class YAML implements AggregationCache.YAML {
        public static final String TYPE = "!in-memory-cache";

        @Override
        public AggregationCache build(String context)
                throws ValidationException {
            return new InMemoryAggregationCache();
        }
    }

    public Map<CacheKey, List<DataPoint>> cache = new HashMap<CacheKey, List<DataPoint>>();

    @Override
    public Callback<AggregationCacheResult> query(TimeSerieSlice slice,
            Aggregation aggregation) {
        final List<Long> range = calculateRange(slice, aggregation);

        log.info("Request range: " + range);

        return null;
    }

    private List<Long> calculateRange(TimeSerieSlice slice,
            Aggregation aggregation) {
        final long period = aggregation.getWidth();
        final long end = slice.getEnd();
        final long start = slice.getStart();

        final long first = start - start % period;
        final long last = end + period - end % period;

        final List<Long> range = new ArrayList<Long>();

        for (long current = first; current < last; current += period) {
            range.add(current);
        }

        return range;
    }
}