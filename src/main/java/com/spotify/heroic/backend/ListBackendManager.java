package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CallbackGroupHandle;
import com.spotify.heroic.async.CallbackStream;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.MetricBackend.DataPointsResult;
import com.spotify.heroic.backend.MetricBackend.FindRowsResult;
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

    public ListBackendManager(List<Backend> backends, long timeout) {
        this.metricBackends = filterMetricBackends(backends);
        this.eventBackends = filterEventBackends(backends);
        this.timeout = timeout;
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
            CallbackGroup.Handle<FindRowsResult> {
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
        public void done(Collection<FindRowsResult> results,
                Collection<Throwable> errors, int cancelled) throws Exception {
            final List<Callback<DataPointsResult>> queries = new LinkedList<Callback<DataPointsResult>>();

            for (final FindRowsResult result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();
                queries.addAll(backend.query(result.getRows(), range));
            }

            final Aggregator.Session session = aggregators.session();

            final CallbackStream<DataPointsResult> callbackStream;

            if (session == null) {
                log.warn("Returning raw results, this will most probably kill your machine!");
                callbackStream = new CallbackStream<DataPointsResult>(queries,
                        new HandleDataPointsAll(callback));
            } else {
                callbackStream = new CallbackStream<DataPointsResult>(queries,
                        new HandleDataPoints(callback, session));
            }

            callback.register(new Callback.Cancelled() {
                @Override
                public void cancel() throws Exception {
                    callbackStream.cancel();
                }
            });
        }
    }

    private final class HandleDataPointsAll implements
            CallbackStream.Handle<DataPointsResult> {
        private final Callback<QueryMetricsResult> callback;
        private final Queue<DataPointsResult> results = new ConcurrentLinkedQueue<DataPointsResult>();

        private HandleDataPointsAll(Callback<QueryMetricsResult> callback) {
            this.callback = callback;
        }

        @Override
        public void finish(Callback<DataPointsResult> callback,
                DataPointsResult result) throws Exception {
            results.add(result);
        }

        @Override
        public void error(Callback<DataPointsResult> callback, Throwable error)
                throws Exception {
            log.error("Result failed: " + error, error);
        }

        @Override
        public void cancel(Callback<DataPointsResult> callback)
                throws Exception {
        }

        @Override
        public void done(int successful, int failed, int cancelled)
                throws Exception {
            final List<DataPoint> datapoints = joinRawResults();

            final RowStatistics rowStatistics = new RowStatistics(successful,
                    failed, cancelled);

            callback.finish(new QueryMetricsResult(datapoints, datapoints
                    .size(), 0, rowStatistics));
        }

        private List<DataPoint> joinRawResults() {
            final List<DataPoint> datapoints = new ArrayList<DataPoint>();

            for (final DataPointsResult result : results) {
                datapoints.addAll(result.getDatapoints());
            }

            Collections.sort(datapoints);
            return datapoints;
        }
    }

    private final class HandleDataPoints implements
            CallbackStream.Handle<DataPointsResult> {
        private final Callback<QueryMetricsResult> callback;
        private final Aggregator.Session session;

        private HandleDataPoints(Callback<QueryMetricsResult> callback,
                Aggregator.Session session) {
            this.callback = callback;
            this.session = session;
        }

        @Override
        public void finish(Callback<DataPointsResult> callback,
                DataPointsResult result) throws Exception {
            session.stream(result.getDatapoints());
        }

        @Override
        public void error(Callback<DataPointsResult> callback, Throwable error)
                throws Exception {
            log.error("Result failed: " + error, error);
        }

        @Override
        public void cancel(Callback<DataPointsResult> callback)
                throws Exception {
        }

        @Override
        public void done(int successful, int failed, int cancelled)
                throws Exception {
            final Aggregator.Result result = session.result();
            final RowStatistics rowStatistics = new RowStatistics(successful,
                    failed, cancelled);
            callback.finish(new QueryMetricsResult(result.getResult(), result
                    .getSampleSize(), result.getOutOfBounds(), rowStatistics));
        }
    }

    @Override
    public Callback<QueryMetricsResult> queryMetrics(final MetricsQuery query)
            throws QueryException {
        final List<Callback<FindRowsResult>> queries = new ArrayList<Callback<FindRowsResult>>();

        final List<Aggregator.Definition> definitions = query.getAggregators();
        final String key = query.getKey();
        final DateRange queryRange = query.getRange();
        final Date start = queryRange.start();
        final Date end = queryRange.end();

        if (!end.after(start)) {
            throw new QueryException("End time must come after start");
        }

        final AggregatorGroup aggregators = new AggregatorGroup(
                buildAggregators(definitions, start, end));

        final DateRange range = calculateDateRange(aggregators, queryRange);

        final Map<String, String> filter = query.getTags();

        for (final MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.findRows(key, range, filter));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final Callback<QueryMetricsResult> callback = new ConcurrentCallback<QueryMetricsResult>();

        final CallbackGroup<FindRowsResult> callbackGroup = new CallbackGroup<FindRowsResult>(
                queries, new HandleFindRowsResult(
                range, callback, aggregators));
        
        callback.register(new Callback.Cancelled() {
            @Override
            public void cancel() throws Exception {
                callbackGroup.cancel();
            }
        });

        return callback;
    }

    private List<Aggregator> buildAggregators(
            List<Aggregator.Definition> definitions, Date start, Date end) {
        final List<Aggregator> aggregators = new ArrayList<Aggregator>();

        if (definitions == null) {
            return aggregators;
        }

        for (Aggregator.Definition definition : definitions) {
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
                Collection<Throwable> errors, int cancelled) throws Exception {
            final Set<TimeSerie> result = new HashSet<TimeSerie>();

            for (final MetricBackend.GetAllRowsResult backendResult : results) {
                final Map<String, List<DataPointsRowKey>> rows = backendResult
                        .getRows();

                for (final Map.Entry<String, List<DataPointsRowKey>> entry : rows
                        .entrySet()) {
                    for (final DataPointsRowKey rowKey : entry.getValue()) {
                        result.add(new TimeSerie(rowKey.getMetricName(),
                                rowKey.getTags()));
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

        new CallbackGroup<MetricBackend.GetAllRowsResult>(queries,
                new HandleGetAllTimeSeries(handle));
        return handle;
    }
}
