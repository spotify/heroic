package com.spotify.heroic.cache;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.cache.model.CachePutResult;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;
import com.spotify.heroic.statistics.AggregationCacheReporter;

@RequiredArgsConstructor
public class AggregationCache {
    private final AggregationCacheReporter reporter;

    private final AggregationCacheBackend backend;

    @RequiredArgsConstructor
    private static final class BackendCacheGetHandle implements
            Callback.Handle<CacheBackendGetResult> {
        private final AggregationCacheReporter reporter;
        private final Callback<CacheQueryResult> callback;
        private final TimeSerieSlice slice;
        private final AggregationGroup aggregation;

        @Override
        public void cancelled(CancelReason reason) throws Exception {
            callback.cancel(reason);
        }

        @Override
        public void failed(Exception e) throws Exception {
            callback.fail(e);
        }

        @Override
        public void resolved(CacheBackendGetResult result) throws Exception {
            final long width = aggregation.getSampling().getSize();

            final List<TimeSerieSlice> misses = new ArrayList<TimeSerieSlice>();
            final List<DataPoint> datapoints = new ArrayList<DataPoint>();
            final List<DataPoint> cached = result.getDatapoints();

            if (width == 0 || cached.isEmpty()) {
                misses.add(slice);
                callback.resolve(new CacheQueryResult(slice, aggregation,
                        datapoints, misses));
                reporter.reportGetMisses(misses.size());
                return;
            }

            final DateRange range = slice.getRange();
            final long end = range.getEnd() - range.getEnd() % width;
            long expected = range.getStart() - range.getStart() % width;

            for (final DataPoint d : result.getDatapoints()) {
                final long start = expected;

                while (expected != d.getTimestamp() && expected < end) {
                    expected += width;
                }

                if (expected != start)
                    misses.add(slice.modify(start, expected));

                if (expected == d.getTimestamp())
                    datapoints.add(d);

                expected += width;
            }

            if (expected != end)
                misses.add(slice.modify(expected, end));

            reporter.reportGetMisses(misses.size());
            callback.resolve(new CacheQueryResult(slice, aggregation, datapoints, misses));
        }
    }

    @RequiredArgsConstructor
    private final class BackendCachePutHandle implements
    Callback.Handle<CacheBackendPutResult> {
        private final Callback<CachePutResult> callback;

        @Override
        public void cancelled(CancelReason reason) throws Exception {
            callback.cancel(reason);
        }

        @Override
        public void failed(Exception e) throws Exception {
            callback.fail(e);
        }

        @Override
        public void resolved(CacheBackendPutResult result)
                throws Exception {
            callback.resolve(new CachePutResult());
        }
    }

    public Callback<CacheQueryResult> get(TimeSerieSlice slice,
            final AggregationGroup aggregation) {
        final CacheBackendKey key = new CacheBackendKey(slice.getTimeSerie(),
                aggregation);
        final Callback<CacheQueryResult> callback = new ConcurrentCallback<CacheQueryResult>();
        final DateRange range = slice.getRange();

        try {
            backend.get(key, range).register(new BackendCacheGetHandle(reporter, callback, slice, aggregation));
        } catch (AggregationCacheException e) {
            callback.fail(e);
        }

        return callback;
    }

    public Callback<CachePutResult> put(TimeSerie timeSerie,
            AggregationGroup aggregation, List<DataPoint> datapoints) {
        final CacheBackendKey key = new CacheBackendKey(timeSerie, aggregation);
        final Callback<CachePutResult> callback = new ConcurrentCallback<CachePutResult>();

        try {
            backend.put(key, datapoints).register(new BackendCachePutHandle(callback)).register(reporter.reportPut());
        } catch (AggregationCacheException e) {
            callback.fail(e);
        }

        return callback;
    }
}
