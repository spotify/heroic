package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.backend.MetricBackend.DataPointsResult;
import com.spotify.heroic.backend.MetricBackend.FindRowsResult;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.Aggregator;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.query.MetricsResponse;

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

        for (Backend backend : backends) {
            if (backend instanceof EventBackend)
                eventBackends.add((EventBackend) backend);
        }

        return eventBackends;
    }

    private List<MetricBackend> filterMetricBackends(List<Backend> backends) {
        final List<MetricBackend> metricBackends = new ArrayList<MetricBackend>();

        for (Backend backend : backends) {
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

        for (Aggregator aggregator : aggregators) {
            datapoints = aggregator.aggregate(start, end, datapoints);
        }

        return datapoints;
    }

    private final class HandleFindRowsResult implements
            GroupQuery.Handle<FindRowsResult> {
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
            final List<Query<DataPointsResult>> queries = new LinkedList<Query<DataPointsResult>>();

            for (FindRowsResult result : results) {
                if (result.isEmpty())
                    continue;

                final MetricBackend backend = result.getBackend();

                queries.addAll(backend.query(result.getRows(), range));
            }

            final GroupQuery<DataPointsResult> group = new GroupQuery<DataPointsResult>(
                    queries);
            group.listen(new HandleDataPointsResult(query, response));
        }
    }

    private final class HandleDataPointsResult implements
            GroupQuery.Handle<DataPointsResult> {
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
            for (Throwable error : errors) {
                log.error("Failed: " + errors.size(), error);
            }

            List<DataPoint> datapoints = new ArrayList<DataPoint>();

            for (DataPointsResult result : results) {
                datapoints.addAll(result.getDatapoints());
            }

            Collections.sort(datapoints);

            int sampleSize = datapoints.size();

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
        final List<Query<FindRowsResult>> queries = new ArrayList<Query<FindRowsResult>>();

        final String key = query.getKey();
        final DateRange range = query.getRange();
        final Map<String, String> filter = query.getAttributes();

        for (MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.findRows(key, range, filter));
            } catch (Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final GroupQuery<FindRowsResult> group = new GroupQuery<FindRowsResult>(
                queries);

        group.listen(new HandleFindRowsResult(query, range, response));
    }
}
