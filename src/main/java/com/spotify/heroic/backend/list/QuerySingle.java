package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.Getter;
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
import com.spotify.heroic.backend.RowStatistics;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.model.FetchDataPoints;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.model.CachePutResult;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.DataPoint;
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

        final Callback<CacheQueryResult> cacheCallback = checkCache(criteria,
                aggregators);

        if (cacheCallback != null) {
            return executeSingleWithCache(criteria, aggregators, cacheCallback);
        }

        return executeSingle(criteria, aggregators);
    }

    private static final class HandleCacheMisses implements
            Callback.Reducer<QueryMetricsResult, QueryMetricsResult> {
        private static final class JoinResult {
            @Getter
            final Map<Map<String, String>, Map<Long, DataPoint>> groups;
            @Getter
            final Map<TimeSerie, List<DataPoint>> cacheUpdates;
            @Getter
            final long sampleSize;
            @Getter
            final long outOfBounds;
            @Getter
            final RowStatistics statistics;

            public JoinResult(
                    Map<Map<String, String>, Map<Long, DataPoint>> groups,
                    Map<TimeSerie, List<DataPoint>> cacheUpdates,
                    long sampleSize, long outOfBounds, RowStatistics statistics) {
                this.groups = groups;
                this.cacheUpdates = cacheUpdates;
                this.sampleSize = sampleSize;
                this.outOfBounds = outOfBounds;
                this.statistics = statistics;
            }
        }

        private final AggregationCache cache;
        private final CacheQueryResult cacheResult;

        public HandleCacheMisses(AggregationCache cache,
                CacheQueryResult cacheResult) {
            this.cache = cache;
            this.cacheResult = cacheResult;
        }

        @Override
        public QueryMetricsResult done(Collection<QueryMetricsResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {

            final JoinResult joinResults = joinResults(results);
            final List<DataPointGroup> groups = buildDataPointGroups(joinResults);

            updateCache(joinResults.getCacheUpdates());

            return new QueryMetricsResult(groups, joinResults.getSampleSize(),
                    joinResults.getOutOfBounds(), joinResults.getStatistics());
        }

        private List<Callback<CachePutResult>> updateCache(
                Map<TimeSerie, List<DataPoint>> cacheUpdates) {
            List<Callback<CachePutResult>> queries = new ArrayList<Callback<CachePutResult>>(
                    cacheUpdates.size());

            for (Map.Entry<TimeSerie, List<DataPoint>> update : cacheUpdates
                    .entrySet()) {
                final TimeSerie timeSerie = update.getKey();
                final List<DataPoint> datapoints = update.getValue();
                queries.add(cache.put(timeSerie, cacheResult.getAggregation(),
                        datapoints));
            }

            return queries;
        }

        private List<DataPointGroup> buildDataPointGroups(JoinResult joinResult) {
            final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();

            for (Map.Entry<Map<String, String>, Map<Long, DataPoint>> entry : joinResult
                    .getGroups().entrySet()) {
                final Map<String, String> tags = entry.getKey();
                final List<DataPoint> datapoints = new ArrayList<DataPoint>(
                        entry.getValue().values());

                Collections.sort(datapoints);

                groups.add(new DataPointGroup(tags, datapoints));
            }

            return groups;
        }

        /**
         * Use a map from <code>{tags -> {long -> DataPoint}}</code> to
         * deduplicate overlapping datapoints.
         * 
         * These overlaps are called 'cache duplicates', which mean that we've
         * somehow managed to fetch duplicate entries from one of.
         * 
         * <ul>
         * <li>The cache backend.</li>
         * <li>The raw backends.</li>
         * </ul>
         * 
         * While this contributes to unecessary overhead, it's not the end of
         * the world. These duplicates are reported as cacheDuplicates in
         * RowStatistics.
         */
        private JoinResult joinResults(Collection<QueryMetricsResult> results) {
            final Map<Map<String, String>, Map<Long, DataPoint>> resultGroups = new HashMap<Map<String, String>, Map<Long, DataPoint>>();
            final Map<TimeSerie, List<DataPoint>> cacheUpdates = new HashMap<TimeSerie, List<DataPoint>>();

            long sampleSize = 0;
            long outOfBounds = 0;

            int rowsSuccessful = 0;
            int rowsFailed = 0;
            int rowsCancelled = 0;

            int cacheDuplicates = addCachedResults(resultGroups);

            for (final QueryMetricsResult result : results) {
                sampleSize += result.getSampleSize();
                outOfBounds += result.getOutOfBounds();

                final RowStatistics statistics = result.getRowStatistics();
                rowsSuccessful += statistics.getSuccessful();
                rowsFailed += statistics.getFailed();
                rowsCancelled += statistics.getCancelled();

                for (final DataPointGroup group : result.getGroups()) {
                    Map<Long, DataPoint> resultSet = resultGroups.get(group
                            .getTags());

                    if (resultSet == null) {
                        resultSet = new HashMap<Long, DataPoint>();
                        resultGroups.put(group.getTags(), resultSet);
                    }

                    for (final DataPoint d : group.getDatapoints()) {
                        if (resultSet.put(d.getTimestamp(), d) != null) {
                            cacheDuplicates += 1;
                        }
                    }

                    cacheUpdates.put(cacheResult.getTimeSerie(),
                            group.getDatapoints());
                }
            }

            final RowStatistics rowStatistics = new RowStatistics(
                    rowsSuccessful, rowsFailed, rowsCancelled, cacheDuplicates);

            return new JoinResult(resultGroups, cacheUpdates, sampleSize,
                    outOfBounds, rowStatistics);
        }

        /**
         * Add the results previously retrieved from cache.
         * 
         * @param resultGroups
         * @return
         */
        private int addCachedResults(
                final Map<Map<String, String>, Map<Long, DataPoint>> resultGroups) {
            int cacheDuplicates = 0;

            final Map<Long, DataPoint> resultSet = new HashMap<Long, DataPoint>();

            for (final DataPoint d : cacheResult.getResult()) {
                if (resultSet.put(d.getTimestamp(), d) != null) {
                    cacheDuplicates += 1;
                }
            }

            resultGroups.put(cacheResult.getTimeSerie().getTags(), resultSet);
            return cacheDuplicates;
        }
    }

    private Callback<QueryMetricsResult> executeSingleWithCache(
            final FindRows original, final AggregatorGroup aggregators,
            final Callback<CacheQueryResult> cacheCallback) {

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        cacheCallback.register(new Callback.Handle<CacheQueryResult>() {
            @Override
            public void cancel(CancelReason reason) throws Exception {
                callback.cancel(reason);
            }

            @Override
            public void error(Throwable e) throws Exception {
                callback.fail(e);
            }

            @Override
            public void finish(final CacheQueryResult cacheResult)
                    throws Exception {
                final List<Callback<QueryMetricsResult>> missQueries = new ArrayList<Callback<QueryMetricsResult>>();

                for (TimeSerieSlice slice : cacheResult.getMisses()) {
                    missQueries.add(executeSingle(
                            original.withRange(slice.getRange()), aggregators));
                }

                /**
                 * EVERYTHING in cache!
                 */
                if (missQueries.isEmpty()) {
                    final DataPointGroup group = new DataPointGroup(original
                            .getFilter(), cacheResult.getResult());
                    final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();

                    groups.add(group);

                    callback.finish(new QueryMetricsResult(groups, 0, 0, null));
                    return;
                }

                /**
                 * Merge with actual queried data.
                 */
                callback.reduce(missQueries, timer, new HandleCacheMisses(
                        cache, cacheResult));
            }
        });

        return callback;
    }

    private Callback<QueryMetricsResult> executeSingle(FindRows criteria,
            AggregatorGroup aggregators) {
        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

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

        return callback.register(group).register(new Callback.Finishable() {
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
