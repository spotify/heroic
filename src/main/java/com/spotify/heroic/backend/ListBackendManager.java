package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackGroup;
import com.spotify.heroic.async.CallbackGroupHandle;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.MetricBackend.DataPointsResult;
import com.spotify.heroic.backend.MetricBackend.FindRowsResult;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.query.Aggregator;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.query.MetricsResponse;
import com.spotify.heroic.query.TagsQuery;

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

    private List<DataPoint> applyAggregators(final MetricsQuery query,
            List<DataPoint> datapoints) {
        final List<Aggregator> aggregators = query.getAggregators();
        final DateRange range = query.getRange();

        final Date start = range.start();
        final Date end = range.end();

        for (final Aggregator aggregator : aggregators) {
            datapoints = aggregator.aggregate(start, end, datapoints);
        }

        return datapoints;
    }

    private final class HandleFindRowsResult implements
            CallbackGroup.Handle<FindRowsResult> {
        private final MetricsQuery query;
        private final DateRange range;
        private final AsyncResponse response;

        private HandleFindRowsResult(MetricsQuery query, DateRange range,
                AsyncResponse response) {
            this.query = query;
            this.range = range;
            this.response = response;
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
            group.listen(new HandleDataPointsResult(query, response));
        }
    }

    private final class HandleDataPointsResult implements
            CallbackGroup.Handle<DataPointsResult> {
        private final MetricsQuery query;
        private final AsyncResponse response;

        private HandleDataPointsResult(MetricsQuery query,
                AsyncResponse response) {
            this.query = query;
            this.response = response;
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

            if (query.getAggregators() != null) {
                datapoints = applyAggregators(query, datapoints);
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
        final DateRange range = query.getRange();
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

        group.listen(new HandleFindRowsResult(query, range, response));
    }

    /**
     * Handle the result from a FindTags query.
     * 
     * Flattens the result from all backends.
     * 
     * @author udoprog
     */
    private final class HandleGetAllRowsResult
            extends
            CallbackGroupHandle<GetAllRowsResult, MetricBackend.GetAllRowsResult> {

        public HandleGetAllRowsResult(Callback<GetAllRowsResult> query) {
            super(query);
        }

        @Override
        public GetAllRowsResult execute(
                Collection<MetricBackend.GetAllRowsResult> results,
                Collection<Throwable> errors, int cancelled) throws Exception {
            final Map<String, List<DataPointsRowKey>> allResults = new HashMap<String, List<DataPointsRowKey>>();

            for (final MetricBackend.GetAllRowsResult result : results) {
                final Map<String, List<DataPointsRowKey>> rows = result
                        .getRows();

                for (final Map.Entry<String, List<DataPointsRowKey>> entry : rows
                        .entrySet()) {
                    final List<DataPointsRowKey> columns = allResults.get(entry
                            .getKey());

                    if (columns == null) {
                        allResults.put(
                                entry.getKey(),
                                new ArrayList<DataPointsRowKey>(entry
                                        .getValue()));
                    } else {
                        columns.addAll(entry.getValue());
                    }
                }
            }

            return new GetAllRowsResult(allResults);
        }
    }

    @Override
    public void queryTags(TagsQuery query, AsyncResponse response) {

    }

    @Override
    public Callback<GetAllRowsResult> getAllRows() {
        final List<Callback<MetricBackend.GetAllRowsResult>> queries = new ArrayList<Callback<MetricBackend.GetAllRowsResult>>();
        final Callback<GetAllRowsResult> handle = new ConcurrentCallback<GetAllRowsResult>();

        for (final MetricBackend backend : metricBackends) {
            queries.add(backend.getAllRows());
        }

        final CallbackGroup<MetricBackend.GetAllRowsResult> group = new CallbackGroup<MetricBackend.GetAllRowsResult>(
                queries);
        group.listen(new HandleGetAllRowsResult(handle));
        return handle;
    }
}
