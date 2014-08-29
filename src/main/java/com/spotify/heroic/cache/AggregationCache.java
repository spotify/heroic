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
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.SeriesSlice;
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
        private final SeriesSlice slice;
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

            final List<SeriesSlice> misses = new ArrayList<SeriesSlice>();

            final List<DataPoint> cached = result.getDatapoints();

            if (width == 0 || cached.isEmpty()) {
                misses.add(slice);
                callback.resolve(new CacheQueryResult(slice, aggregation,
                        cached, misses));
                reporter.reportGetMiss(misses.size());
                return;
            }

            final DateRange range = slice.getRange();
            final long end = range.getEnd();

            long current = range.getStart();

            for (final DataPoint d : cached) {
                if (current + width != d.getTimestamp()
                        && current < d.getTimestamp())
                    misses.add(slice.modify(current, d.getTimestamp()));

                current = d.getTimestamp();
            }

            if (current < end)
                misses.add(slice.modify(current, end));

            reporter.reportGetMiss(misses.size());
            callback.resolve(new CacheQueryResult(slice, aggregation, cached,
                    misses));
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
        public void resolved(CacheBackendPutResult result) throws Exception {
            callback.resolve(new CachePutResult());
        }
    }

    public Callback<CacheQueryResult> get(SeriesSlice slice,
            final AggregationGroup aggregation) {
        final CacheBackendKey key = new CacheBackendKey(slice.getSeries(),
                aggregation);
        final Callback<CacheQueryResult> callback = new ConcurrentCallback<CacheQueryResult>();
        final DateRange range = slice.getRange();

        try {
            backend.get(key, range).register(
                    new BackendCacheGetHandle(reporter, callback, slice,
                            aggregation));
        } catch (final AggregationCacheException e) {
            callback.fail(e);
        }

        return callback;
    }

    public Callback<CachePutResult> put(Series series,
            AggregationGroup aggregation, List<DataPoint> datapoints) {
        final CacheBackendKey key = new CacheBackendKey(series, aggregation);
        final Callback<CachePutResult> callback = new ConcurrentCallback<CachePutResult>();

        try {
            backend.put(key, datapoints)
                    .register(new BackendCachePutHandle(callback))
                    .register(reporter.reportPut());
        } catch (final AggregationCacheException e) {
            callback.fail(e);
        }

        return callback;
    }
}
