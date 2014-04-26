package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager.DataPointGroup;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.model.FetchDataPoints;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;
import com.spotify.heroic.query.DateRange;

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

    private final class HandleFindRowsResult implements
            CallbackGroup.Handle<FindRows.Result> {
        private final DateRange range;
        private final Callback<QueryMetricsResult> callback;
        private final AggregatorGroup aggregators;

        private HandleFindRowsResult(DateRange range,
                Callback<QueryMetricsResult> callback,
                AggregatorGroup aggregators) {
            this.range = range;
            this.callback = callback;
            this.aggregators = aggregators;
        }

        @Override
        public void done(final Collection<FindRows.Result> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {

            final Aggregator.Session session = aggregators.session();

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
                    counters.add(backend.getColumnCount(row, range));
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
                        .getRows(), range)));
            }

            final Callback.StreamReducer<FetchDataPoints.Result, QueryMetricsResult> reducer;

            if (session == null) {
                reducer = new SimpleCallbackStream(null);
            } else {
                reducer = new AggregatedCallbackStream(null, session);
            }

            callback.reduce(queries, timer, reducer);
        }
    }

    public Callback<QueryMetricsResult> execute(final FindRows criteria,
            final AggregatorGroup aggregators) {

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();
        
        final Callback<CacheQueryResult> cacheCallback = checkCache(
                criteria,
                aggregators);

        if (cacheCallback != null) {
            cacheCallback
                    .register(new Callback.Handle<CacheQueryResult>() {
                        @Override
                        public void cancel(CancelReason reason)
                                throws Exception {
                            callback.cancel(reason);
                        }

                        @Override
                        public void error(Throwable e) throws Exception {
                            callback.fail(e);
                        }

                        @Override
                        public void finish(CacheQueryResult result)
                                throws Exception {
                            final DataPointGroup group = new DataPointGroup(
                                    null, result.getResult());
                            final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();
                            groups.add(group);

                            if (result.getMisses().isEmpty()) {
                                callback.finish(new QueryMetricsResult(groups,
                                        0, 0, null));
                                return;
                            }

                            executeSingle(callback, criteria, aggregators);
                        }
                    });
        }
        
        executeSingle(callback, criteria, aggregators);

        return callback;
    }

    private void executeSingle(
            Callback<QueryMetricsResult> callback, FindRows criteria,
            AggregatorGroup aggregators) {
        final List<Callback<FindRows.Result>> queries = new ArrayList<Callback<FindRows.Result>>();

        for (final MetricBackend backend : backends) {
            try {
                queries.add(backend.findRows(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final DateRange range = criteria.getRange();

        final CallbackGroup<FindRows.Result> group = new CallbackGroup<FindRows.Result>(
                queries, new HandleFindRowsResult(range, callback, aggregators));

        final Timer.Context context = timer.time();

        callback.register(group).register(new Callback.Finishable() {
            @Override
            public void finish() throws Exception {
                context.stop();
            }
        });
    }

    private Callback<CacheQueryResult> checkCache(FindRows criteria,
            AggregatorGroup aggregators) {
        final TimeSerie timeSerie = new TimeSerie(criteria.getKey(),
                criteria.getFilter());
        final TimeSerieSlice slice = timeSerie.slice(criteria.getRange());
        final Aggregation aggregation = aggregators.getAggregation();

        if (cache == null)
            return null;

        return cache.query(slice, aggregation);
    }
}
