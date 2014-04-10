package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CallbackGroupHandle;
import com.spotify.heroic.async.CallbackStream;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.MetricsQuery;

@Slf4j
public class ListBackendManager implements BackendManager {
    @Getter
    private final List<MetricBackend> metricBackends;

    @Getter
    private final List<EventBackend> eventBackends;

    @Getter
    private final long timeout;

    @Getter
    private final long maxAggregationMagnitude;

    private final Timer queryMetricsGroupsTimer;
    private final Timer queryMetricsSingleTimer;

    public ListBackendManager(List<Backend> backends, MetricRegistry registry,
            long timeout, long maxAggregationMagnitude) {
        this.metricBackends = filterMetricBackends(backends);
        this.eventBackends = filterEventBackends(backends);
        this.timeout = timeout;
        this.queryMetricsGroupsTimer = registry.timer(MetricRegistry.name(
                "heroic", "query-metrics", "group"));
        this.queryMetricsSingleTimer = registry.timer(MetricRegistry.name(
                "heroic", "query-metrics", "single"));
        this.maxAggregationMagnitude = maxAggregationMagnitude;
    }

    private List<EventBackend> filterEventBackends(List<Backend> backends) {
        final List<EventBackend> eventBackends = new ArrayList<EventBackend>();

        for (final Backend backend : backends) {
            if (backend instanceof EventBackend)
                eventBackends.add((EventBackend) backend);
        }

        return eventBackends;
    }

    private List<MetricBackend> filterMetricBackends(List<Backend> backends) {
        final List<MetricBackend> metricBackends = new ArrayList<MetricBackend>();

        for (final Backend backend : backends) {
            if (backend instanceof MetricBackend)
                metricBackends.add((MetricBackend) backend);
        }

        return metricBackends;
    }

    private final class HandleFindRowsResult implements
            CallbackGroup.Handle<MetricBackend.FindRowsResult> {
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
        public void done(Collection<MetricBackend.FindRowsResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final List<Callback<MetricBackend.DataPointsResult>> queries = new LinkedList<Callback<MetricBackend.DataPointsResult>>();

            for (final MetricBackend.FindRowsResult result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();
                final long start = System.currentTimeMillis();
                final Long columnCount = backend.getColumnCount(
                        result.getRows(), range, 100000l);
                final long end = System.currentTimeMillis();
                log.warn("We are about to retrieve " + columnCount
                        + " data points from " + backend.toString()
                        + " And it took " + (end - start) + " ms.");
                // queries.addAll(backend.query(result.getRows(), range));
            }

            final CallbackStream<MetricBackend.DataPointsResult> stream;

            final Aggregator.Session session = aggregators.session();

            if (session == null) {
                log.warn("Returning raw results, this will most probably kill your machine!");
                stream = new CallbackStream<MetricBackend.DataPointsResult>(
                        queries, new HandleDataPointsAll(null, callback));
            } else {
                stream = new CallbackStream<MetricBackend.DataPointsResult>(
                        queries, new HandleDataPoints(null, callback, session));
            }

            callback.register(stream);
        }
    }

    private final class HandleDataPointsAll implements
            CallbackStream.Handle<MetricBackend.DataPointsResult> {
        private final Map<String, String> tags;
        private final Callback<QueryMetricsResult> callback;
        private final Queue<MetricBackend.DataPointsResult> results = new ConcurrentLinkedQueue<MetricBackend.DataPointsResult>();

        private HandleDataPointsAll(Map<String, String> tags,
                Callback<QueryMetricsResult> callback) {
            this.tags = tags;
            this.callback = callback;
        }

        @Override
        public void finish(Callback<MetricBackend.DataPointsResult> callback,
                MetricBackend.DataPointsResult result) throws Exception {
            results.add(result);
        }

        @Override
        public void error(Callback<MetricBackend.DataPointsResult> callback,
                Throwable error) throws Exception {
            log.error("Result failed: " + error, error);
        }

        @Override
        public void cancel(Callback<MetricBackend.DataPointsResult> callback,
                CancelReason reason) throws Exception {
        }

        @Override
        public void done(int successful, int failed, int cancelled)
                throws Exception {
            final List<DataPoint> datapoints = joinRawResults();

            final RowStatistics rowStatistics = new RowStatistics(successful,
                    failed, cancelled);

            final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();
            groups.add(new DataPointGroup(tags, datapoints));

            callback.finish(new QueryMetricsResult(groups, datapoints.size(),
                    0, rowStatistics));
        }

        private List<DataPoint> joinRawResults() {
            final List<DataPoint> datapoints = new ArrayList<DataPoint>();

            for (final MetricBackend.DataPointsResult result : results) {
                datapoints.addAll(result.getDatapoints());
            }

            Collections.sort(datapoints);
            return datapoints;
        }
    }

    private final class HandleDataPoints implements
            CallbackStream.Handle<MetricBackend.DataPointsResult> {
        private final Map<String, String> tags;
        private final Callback<QueryMetricsResult> callback;
        private final Aggregator.Session session;

        private HandleDataPoints(Map<String, String> tags,
                Callback<QueryMetricsResult> callback,
                Aggregator.Session session) {
            this.tags = tags;
            this.callback = callback;
            this.session = session;
        }

        @Override
        public void finish(Callback<MetricBackend.DataPointsResult> callback,
                MetricBackend.DataPointsResult result) throws Exception {
            session.stream(result.getDatapoints());
        }

        @Override
        public void error(Callback<MetricBackend.DataPointsResult> callback,
                Throwable error) throws Exception {
            log.error("Result failed: " + error, error);
        }

        @Override
        public void cancel(Callback<MetricBackend.DataPointsResult> callback,
                CancelReason reason) throws Exception {
        }

        @Override
        public void done(int successful, int failed, int cancelled)
                throws Exception {
            final Aggregator.Result result = session.result();
            final RowStatistics rowStatistics = new RowStatistics(successful,
                    failed, cancelled);

            final List<DataPointGroup> groups = new ArrayList<DataPointGroup>();
            groups.add(new DataPointGroup(tags, result.getResult()));

            callback.finish(new QueryMetricsResult(groups, result
                    .getSampleSize(), result.getOutOfBounds(), rowStatistics));
        }
    }

    @Override
    public Callback<QueryMetricsResult> queryMetrics(final MetricsQuery query)
            throws QueryException {
        final List<Aggregator.Definition> definitions = query.getAggregators();
        final String key = query.getKey();
        final DateRange queryRange = query.getRange();
        final Date start = queryRange.start();
        final Date end = queryRange.end();
        final List<String> groupBy = query.getGroupBy();

        if (!end.after(start)) {
            throw new QueryException("End time must come after start");
        }

        final AggregatorGroup aggregators = new AggregatorGroup(
                buildAggregators(definitions, start, end));

        final long memoryMagnitude = aggregators
                .getCalculationMemoryMagnitude();
        if (memoryMagnitude > maxAggregationMagnitude) {
            final Callback<QueryMetricsResult> failedCallback = new ConcurrentCallback<QueryMetricsResult>();
            failedCallback.cancel(new CancelReason(
                    "This query would result in too many datapoints"));
            return failedCallback;
        }

        final DateRange range = calculateDateRange(aggregators, queryRange);

        final Map<String, String> filter = query.getTags();

        final Callback<QueryMetricsResult> callback;
        final Timer.Context context;

        if (groupBy != null && !groupBy.isEmpty()) {
            callback = queryGroups(key, aggregators, range, filter, groupBy);
            context = queryMetricsGroupsTimer.time();
        } else {
            callback = querySingle(key, aggregators, range, filter);
            context = queryMetricsSingleTimer.time();
        }

        return callback.register(new Callback.Ended() {
            @Override
            public void ended() throws Exception {
                context.stop();
            }
        });
    }

    private final class HandleFindRowGroupsResult implements
            CallbackGroup.Handle<MetricBackend.FindRowGroupsResult> {
        private final DateRange range;
        private final Callback<QueryMetricsResult> callback;
        private final AggregatorGroup aggregators;

        private HandleFindRowGroupsResult(DateRange range,
                Callback<QueryMetricsResult> callback,
                AggregatorGroup aggregators) {
            this.range = range;
            this.callback = callback;
            this.aggregators = aggregators;
        }

        @Override
        public void done(Collection<MetricBackend.FindRowGroupsResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> mappedQueries = prepareGroups(results);

            final List<Callback<QueryMetricsResult>> queries = prepareQueries(mappedQueries);

            final CallbackGroup<QueryMetricsResult> group = new CallbackGroup<QueryMetricsResult>(
                    queries, new CallbackGroup.Handle<QueryMetricsResult>() {
                        @Override
                        public void done(
                                Collection<QueryMetricsResult> results,
                                Collection<Throwable> errors,
                                Collection<CancelReason> cancelled)
                                throws Exception {
                            final List<DataPointGroup> groups = new LinkedList<DataPointGroup>();

                            long sampleSize = 0;
                            long outOfBounds = 0;
                            int rowSuccessful = 0;
                            int rowFailed = 0;
                            int rowCancelled = 0;

                            for (final QueryMetricsResult result : results) {
                                sampleSize += result.getSampleSize();
                                outOfBounds += result.getOutOfBounds();

                                final RowStatistics rowStatistics = result
                                        .getRowStatistics();

                                rowSuccessful += rowStatistics.getSuccessful();
                                rowFailed += rowStatistics.getFailed();
                                rowCancelled += rowStatistics.getCancelled();

                                groups.addAll(result.getGroups());
                            }

                            final RowStatistics rowStatistics = new RowStatistics(
                                    rowSuccessful, rowFailed, rowCancelled);

                            callback.finish(new QueryMetricsResult(groups,
                                    sampleSize, outOfBounds, rowStatistics));
                        }
                    });

            callback.register(group);
        }

        private List<Callback<QueryMetricsResult>> prepareQueries(
                final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> mappedQueries)
                throws Exception {
            final List<Callback<QueryMetricsResult>> queries = new ArrayList<Callback<QueryMetricsResult>>();

            for (final Map.Entry<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> entry : mappedQueries
                    .entrySet()) {
                final Map<String, String> tags = entry.getKey();
                final List<Callback<MetricBackend.DataPointsResult>> callbacks = entry
                        .getValue();

                final Aggregator.Session session = aggregators.session();

                final CallbackStream<MetricBackend.DataPointsResult> callbackStream;

                final Callback<QueryMetricsResult> partial = new ConcurrentCallback<QueryMetricsResult>();

                if (session == null) {
                    callbackStream = new CallbackStream<MetricBackend.DataPointsResult>(
                            callbacks, new HandleDataPointsAll(tags, partial));
                } else {
                    callbackStream = new CallbackStream<MetricBackend.DataPointsResult>(
                            callbacks, new HandleDataPoints(tags, partial,
                                    session));
                }

                partial.register(new Callback.Cancelled() {
                    @Override
                    public void cancel(CancelReason reason) throws Exception {
                        callbackStream.cancel(reason);
                    }
                });

                queries.add(partial);
            }

            return queries;
        }

        private final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> prepareGroups(
                Collection<MetricBackend.FindRowGroupsResult> results)
                throws QueryException {

            final Map<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>> mappedQueries = new HashMap<Map<String, String>, List<Callback<MetricBackend.DataPointsResult>>>();

            for (final MetricBackend.FindRowGroupsResult result : results) {
                final MetricBackend backend = result.getBackend();

                for (final Map.Entry<Map<String, String>, List<DataPointsRowKey>> entry : result
                        .getRowGroups().entrySet()) {
                    final Map<String, String> tags = entry.getKey();
                    final List<DataPointsRowKey> rows = entry.getValue();

                    List<Callback<MetricBackend.DataPointsResult>> callbacks = mappedQueries
                            .get(tags);

                    if (callbacks == null) {
                        callbacks = new ArrayList<Callback<MetricBackend.DataPointsResult>>();
                        mappedQueries.put(tags, callbacks);
                    }

                    callbacks.addAll(backend.query(rows, range));
                }
            }

            return mappedQueries;
        }
    }

    private Callback<QueryMetricsResult> queryGroups(final String key,
            final AggregatorGroup aggregators, final DateRange range,
            final Map<String, String> filter, List<String> groupBy) {

        final List<Callback<MetricBackend.FindRowGroupsResult>> queries = new ArrayList<Callback<MetricBackend.FindRowGroupsResult>>();

        final MetricBackend.FindRowGroups query = new MetricBackend.FindRowGroups(
                key, range, filter, groupBy);

        for (final MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.findRowGroups(query));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        final CallbackGroup<MetricBackend.FindRowGroupsResult> group = new CallbackGroup<MetricBackend.FindRowGroupsResult>(
                queries, new HandleFindRowGroupsResult(range, callback,
                        aggregators));

        return callback.register(group);
    }

    private Callback<QueryMetricsResult> querySingle(final String key,
            final AggregatorGroup aggregators, final DateRange range,
            final Map<String, String> filter) {
        final List<Callback<MetricBackend.FindRowsResult>> queries = new ArrayList<Callback<MetricBackend.FindRowsResult>>();

        final MetricBackend.FindRows query = new MetricBackend.FindRows(key,
                filter, range);

        for (final MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.findRows(query));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        return callback
                .register(new CallbackGroup<MetricBackend.FindRowsResult>(
                        queries, new HandleFindRowsResult(range, callback,
                                aggregators)));
    }

    private List<Aggregator> buildAggregators(
            List<Aggregator.Definition> definitions, Date start, Date end) {
        final List<Aggregator> aggregators = new ArrayList<Aggregator>();

        if (definitions == null) {
            return aggregators;
        }

        for (final Aggregator.Definition definition : definitions) {
            aggregators.add(definition.build(start.getTime(), end.getTime()));
        }

        return aggregators;
    }

    /**
     * Check if the query wants to hint at a specific interval. If that is the
     * case, round the provided date to the specified interval.
     * 
     * @param query
     * @return
     */
    private DateRange calculateDateRange(AggregatorGroup aggregators,
            DateRange range) {
        final long hint = aggregators.getIntervalHint();

        if (hint > 0) {
            return range.roundToInterval(hint);
        } else {
            return range;
        }
    }

    /**
     * Handle the result from a FindTags query.
     * 
     * Flattens the result from all backends.
     * 
     * @author udoprog
     */
    private final class HandleGetAllTimeSeries
            extends
            CallbackGroupHandle<GetAllTimeSeriesResult, MetricBackend.GetAllRowsResult> {

        public HandleGetAllTimeSeries(Callback<GetAllTimeSeriesResult> query) {
            super(query);
        }

        @Override
        public GetAllTimeSeriesResult execute(
                Collection<MetricBackend.GetAllRowsResult> results,
                Collection<Throwable> errors, Collection<CancelReason> cancelled)
                throws Exception {
            final Set<TimeSerie> result = new HashSet<TimeSerie>();

            for (final MetricBackend.GetAllRowsResult backendResult : results) {
                final Map<String, List<DataPointsRowKey>> rows = backendResult
                        .getRows();

                for (final Map.Entry<String, List<DataPointsRowKey>> entry : rows
                        .entrySet()) {
                    for (final DataPointsRowKey rowKey : entry.getValue()) {
                        result.add(new TimeSerie(rowKey,
                                rowKey.getMetricName(), rowKey.getTags()));
                    }
                }
            }

            return new GetAllTimeSeriesResult(result);
        }
    }

    @Override
    public Callback<GetAllTimeSeriesResult> getAllRows() {
        final List<Callback<MetricBackend.GetAllRowsResult>> queries = new ArrayList<Callback<MetricBackend.GetAllRowsResult>>();
        final Callback<GetAllTimeSeriesResult> handle = new ConcurrentCallback<GetAllTimeSeriesResult>();

        for (final MetricBackend backend : metricBackends) {
            queries.add(backend.getAllRows());
        }

        final CallbackGroup<MetricBackend.GetAllRowsResult> group = new CallbackGroup<MetricBackend.GetAllRowsResult>(
                queries, new HandleGetAllTimeSeries(handle));

        handle.register(group);

        return handle;
    }
}
