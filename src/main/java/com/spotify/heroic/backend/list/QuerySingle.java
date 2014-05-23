package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CallbackHandle;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.DataPointGroup;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.RowStatistics;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.model.FetchDataPoints;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

@Slf4j
public class QuerySingle {
    private final List<MetricBackend> backends;
    private final Timer timer;
    private final long maxQueriableDataPoints;
    private final AggregationCache cache;

    public QuerySingle(List<MetricBackend> backends, Timer timer,
            long maxQueriableDataPoints, AggregationCache cache) {
        this.backends = backends;
        this.timer = timer;
        this.maxQueriableDataPoints = maxQueriableDataPoints;
        this.cache = cache;
    }

    @RequiredArgsConstructor
    private final class HandleFindRowsResult implements
            CallbackGroup.Handle<FindRows.Result> {
        private final TimeSerieSlice slice;
        private final Callback<QueryMetricsResult> callback;
        private final AggregatorGroup aggregator;

        @Override
        public void done(final Collection<FindRows.Result> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {

            final Aggregator.Session session = aggregator.session(slice.getRange());

            if (session == null) {
                countTheProcessDataPoints(results);
                return;
            }

            processDataPoints(results, session);
        }

        /**
         * Performs processDataPoints only if the amount of data points selected
         * does not exceed {@link #maxQueriableDataPoints}
         * 
         * @param results
         */
        private void countTheProcessDataPoints(
                final Collection<FindRows.Result> results) {
            final List<Callback<Long>> counters = buildCountRequests(results);

            final Callback<Void> check = new ConcurrentCallback<Void>();

            check.reduce(counters, timer, new CountThresholdCallbackStream(
                    maxQueriableDataPoints, check));

            check.register(new Callback.Handle<Void>() {
                @Override
                public void cancel(CancelReason reason) throws Exception {
                    callback.cancel(reason);
                }

                @Override
                public void error(Throwable e) throws Exception {
                    callback.fail(e);
                }

                @Override
                public void finish(Void result) throws Exception {
                    processDataPoints(results, null);
                }
            });
        }

        /**
         * Sets up count requests for all the collected results.
         * 
         * @param results
         * @return
         */
        private List<Callback<Long>> buildCountRequests(
                final Collection<FindRows.Result> results) {
            final List<Callback<Long>> counters = new LinkedList<Callback<Long>>();

            for (final FindRows.Result result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();

                for (final DataPointsRowKey row : result.getRows()) {
                    counters.add(backend.getColumnCount(row, slice.getRange()));
                }
            }
            return counters;
        }

        private void processDataPoints(Collection<FindRows.Result> results,
                final Aggregator.Session session) {
            final List<Callback<FetchDataPoints.Result>> queries = new LinkedList<Callback<FetchDataPoints.Result>>();
            for (final FindRows.Result result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();
                queries.addAll(backend.query(new FetchDataPoints(result
                        .getRows(), slice.getRange())));
            }

            final Callback.StreamReducer<FetchDataPoints.Result, QueryMetricsResult> reducer;

            if (session == null) {
                reducer = new SimpleCallbackStream(slice);
            } else {
                reducer = new AggregatedCallbackStream(slice, session);
            }

            callback.reduce(queries, timer, reducer);
        }
    }

    public Callback<QueryMetricsResult> execute(final FindRows criteria,
            final AggregatorGroup aggregator) {

        final Callback<CacheQueryResult> cacheCallback = checkCache(criteria,
                aggregator);

        if (cacheCallback != null) {
            return executeSingleWithCache(criteria, aggregator, cacheCallback);
        }

        return executeSingle(criteria, aggregator);
    }

    private Callback<QueryMetricsResult> executeSingleWithCache(
            final FindRows original, final AggregatorGroup aggregator,
            final Callback<CacheQueryResult> cacheCallback) {

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        cacheCallback.register(new CallbackHandle<CacheQueryResult, QueryMetricsResult>("single.cache-query", timer, callback) {
            public void execute(Callback<QueryMetricsResult> callback, final CacheQueryResult cacheResult) {
                final List<Callback<QueryMetricsResult>> missQueries = new ArrayList<Callback<QueryMetricsResult>>();

                for (TimeSerieSlice slice : cacheResult.getMisses()) {
                    missQueries.add(executeSingle(
                            original.withRange(slice.getRange()), aggregator));
                }

                /**
                 * EVERYTHING in cache!
                 */
                if (missQueries.isEmpty()) {
                    final DataPointGroup group = new DataPointGroup(original
                            .getFilter(), cacheResult.getResult());
                    final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();

                    groups.add(group);

                    callback.finish(new QueryMetricsResult(groups, 0, 0, new RowStatistics(0, 0, 0)));
                    return;
                }

                /**
                 * Merge with actual queried data.
                 */
                callback.reduce(missQueries, timer, new CacheMissMerger(
                        cache, cacheResult, true));
            }
        });

        return callback;
    }

    private Callback<QueryMetricsResult> executeSingle(FindRows criteria,
            AggregatorGroup aggregator) {
        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        final List<Callback<FindRows.Result>> queries = new ArrayList<Callback<FindRows.Result>>();

        for (final MetricBackend backend : backends) {
            try {
                queries.add(backend.findRows(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final TimeSerie timeSerie = new TimeSerie(criteria.getKey(), criteria.getFilter());
        final DateRange range = criteria.getRange();
        final TimeSerieSlice slice = new TimeSerieSlice(timeSerie, range);

        final CallbackGroup<FindRows.Result> group = new CallbackGroup<FindRows.Result>(
                queries, new HandleFindRowsResult(slice, callback, aggregator));

        final Timer.Context context = timer.time();

        return callback.register(group).register(new Callback.Finishable() {
            @Override
            public void finish() throws Exception {
                context.stop();
            }
        });
    }

    private Callback<CacheQueryResult> checkCache(FindRows criteria,
            AggregatorGroup aggregator) {
        if (cache == null)
            return null;

        final TimeSerie timeSerie = new TimeSerie(criteria.getKey(),
                criteria.getFilter());
        final TimeSerieSlice slice = timeSerie.slice(criteria.getRange());
        return cache.get(slice, aggregator);
    }
}
