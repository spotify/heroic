package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CallbackGroupHandle;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.MetricBackend.DataPointsResult;
import com.spotify.heroic.backend.MetricBackend.FindRowsResult;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.query.MetricsResponse;
import com.spotify.heroic.query.TimeSeriesQuery;

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
        private final MetricsQuery query;
        private final DateRange range;
        private final AsyncResponse response;
        private final AggregatorGroup aggregators;

        private HandleFindRowsResult(MetricsQuery query, DateRange range,
                AsyncResponse response, AggregatorGroup aggregators) {
            this.query = query;
            this.range = range;
            this.response = response;
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

            final CallbackGroup<DataPointsResult> group = new CallbackGroup<DataPointsResult>(
                    queries);
            group.listen(new HandleDataPointsResult(query, response,
                    aggregators));
        }
    }

    private final class HandleDataPointsResult implements
            CallbackGroup.Handle<DataPointsResult> {
        private final MetricsQuery query;
        private final AsyncResponse response;
        private final AggregatorGroup aggregators;

        private HandleDataPointsResult(MetricsQuery query,
                AsyncResponse response, AggregatorGroup aggregators) {
            this.query = query;
            this.response = response;
            this.aggregators = aggregators;
        }

        @Override
        public void done(Collection<DataPointsResult> results,
                Collection<Throwable> errors, int cancelled) throws Exception {
            log.info("Query Result: results:{} errors:{} cancelled:{}",
                    results.size(), errors.size(), cancelled);

            for (final Throwable error : errors) {
                log.error("Failed: " + errors.size(), error);
            }

            List<DataPoint> datapoints = new ArrayList<DataPoint>();

            for (final DataPointsResult result : results) {
                datapoints.addAll(result.getDatapoints());
            }

            Collections.sort(datapoints);

            final int sampleSize = datapoints.size();

            if (!aggregators.isEmpty()) {
                datapoints = aggregators.aggregate(datapoints);
            }

            final MetricsResponse metricsResponse = new MetricsResponse(
                    datapoints, sampleSize);

            response.resume(Response.status(Response.Status.OK)
                    .entity(metricsResponse).build());
        }
    }

    @Override
    public void queryMetrics(final MetricsQuery query,
            final AsyncResponse response) {
        final List<Callback<FindRowsResult>> queries = new ArrayList<Callback<FindRowsResult>>();

        final String key = query.getKey();

        final AggregatorGroup aggregators = new AggregatorGroup(
                buildAggregators(query));

        final DateRange range = calculateDateRange(aggregators, query);

        final Map<String, String> filter = query.getTags();

        for (final MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.findRows(key, range, filter));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final CallbackGroup<FindRowsResult> group = new CallbackGroup<FindRowsResult>(
                queries);

        group.listen(new HandleFindRowsResult(query, range, response,
                aggregators));
    }

    private List<Aggregator> buildAggregators(MetricsQuery query) {
        final List<Aggregator> aggregators = new ArrayList<Aggregator>();

        if (query.getAggregators() == null) {
            return aggregators;
        }

        final DateRange range = query.getRange();
        final Date start = range.start();
        final Date end = range.end();

        for (Aggregator.JSON factory : query.getAggregators()) {
            aggregators.add(factory.build(start, end));
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
            final MetricsQuery query) {
        final DateRange range = query.getRange();
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
    public void queryTags(TimeSeriesQuery query, AsyncResponse response) {

    }

    @Override
    public Callback<GetAllTimeSeriesResult> getAllRows() {
        final List<Callback<MetricBackend.GetAllRowsResult>> queries = new ArrayList<Callback<MetricBackend.GetAllRowsResult>>();
        final Callback<GetAllTimeSeriesResult> handle = new ConcurrentCallback<GetAllTimeSeriesResult>();

        for (final MetricBackend backend : metricBackends) {
            queries.add(backend.getAllRows());
        }

        final CallbackGroup<MetricBackend.GetAllRowsResult> group = new CallbackGroup<MetricBackend.GetAllRowsResult>(
                queries);
        group.listen(new HandleGetAllTimeSeries(handle));
        return handle;
    }
}
