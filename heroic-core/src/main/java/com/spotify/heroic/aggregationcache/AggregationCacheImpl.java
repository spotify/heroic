package com.spotify.heroic.aggregationcache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregationcache.model.CacheBackendGetResult;
import com.spotify.heroic.aggregationcache.model.CacheBackendKey;
import com.spotify.heroic.aggregationcache.model.CacheBackendPutResult;
import com.spotify.heroic.aggregationcache.model.CachePutResult;
import com.spotify.heroic.aggregationcache.model.CacheQueryResult;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.FutureHandle;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.statistics.AggregationCacheReporter;

@NoArgsConstructor
public class AggregationCacheImpl implements AggregationCache {
    @Inject
    private AggregationCacheBackend backend;

    @Inject
    private AggregationCacheReporter reporter;

    @RequiredArgsConstructor
    private static final class BackendCacheGetHandle implements FutureHandle<CacheBackendGetResult> {
        private final AggregationCacheReporter reporter;
        private final Future<CacheQueryResult> callback;
        private final DateRange range;

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
            final CacheBackendKey key = result.getKey();

            final long width = key.getAggregation().getSampling().getSize();

            final List<DateRange> misses = new ArrayList<DateRange>();

            final List<DataPoint> cached = result.getDatapoints();

            if (width == 0 || cached.isEmpty()) {
                misses.add(range);
                callback.resolve(new CacheQueryResult(key, range, cached, misses));
                reporter.reportGetMiss(misses.size());
                return;
            }

            final long end = range.getEnd();

            long current = range.getStart();

            for (final DataPoint d : cached) {
                if (current + width != d.getTimestamp() && current < d.getTimestamp())
                    misses.add(range.modify(current, d.getTimestamp()));

                current = d.getTimestamp();
            }

            if (current < end)
                misses.add(range.modify(current, end));

            reporter.reportGetMiss(misses.size());
            callback.resolve(new CacheQueryResult(key, range, cached, misses));
        }
    }

    @RequiredArgsConstructor
    private final class BackendCachePutHandle implements FutureHandle<CacheBackendPutResult> {
        private final Future<CachePutResult> callback;

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

    @Override
    public boolean isConfigured() {
        return backend != null;
    }

    @Override
    public Future<CacheQueryResult> get(Filter filter, Map<String, String> group, final AggregationGroup aggregation,
            DateRange range) throws CacheOperationException {
        if (!isConfigured())
            throw new CacheOperationException("Cache backend is not configured");

        final CacheBackendKey key = new CacheBackendKey(filter, group, aggregation);
        final Future<CacheQueryResult> callback = Futures.future();

        backend.get(key, range).register(new BackendCacheGetHandle(reporter, callback, range));

        return callback;
    }

    @Override
    public Future<CachePutResult> put(Filter filter, Map<String, String> group, AggregationGroup aggregation,
            List<DataPoint> datapoints) throws CacheOperationException {
        final CacheBackendKey key = new CacheBackendKey(filter, group, aggregation);
        final Future<CachePutResult> callback = Futures.future();

        if (!isConfigured())
            throw new CacheOperationException("Cache backend is not configured");

        backend.put(key, datapoints).register(new BackendCachePutHandle(callback)).register(reporter.reportPut());

        return callback;
    }
}
