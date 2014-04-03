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
import java.util.Set;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.backend.MetricBackend.DataPointsResult;
import com.spotify.heroic.backend.MetricBackend.FindRowsResult;
import com.spotify.heroic.backend.MetricBackend.FindTagsResult;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.Aggregator;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.query.MetricsResponse;
import com.spotify.heroic.query.TagsQuery;
import com.spotify.heroic.query.TagsResponse;

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
            log.info("Query Result: results:{} errors:{} cancelled:{}", results.size(),
                    errors.size(), cancelled);

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
        final Map<String, String> filter = query.getTags();

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

    /**
     * Handle the result from a FindTags query.
     * 
     * Flattens the result from all backends.
     * 
     * @author udoprog
     */
    private final class HandleFindTagsResult implements
            GroupQuery.Handle<FindTagsResult> {
        private final AsyncResponse response;

        private HandleFindTagsResult(AsyncResponse response) {
            this.response = response;
        }

        @Override
        public void done(Collection<FindTagsResult> results,
                Collection<Throwable> errors, int cancelled) throws Exception {
            final Map<String, Set<String>> tags = new HashMap<String, Set<String>>();
            final Set<String> metrics = new HashSet<String>();

            for (final FindTagsResult result : results) {
                final Map<String, Set<String>> partial = result.getTags();

                for (Map.Entry<String, Set<String>> entry : partial.entrySet()) {
                    Set<String> values = tags.get(entry.getKey());

                    if (values == null) {
                        values = new HashSet<String>();
                        tags.put(entry.getKey(), values);
                    }

                    values.addAll(entry.getValue());
                }

                metrics.addAll(result.getMetrics());
            }

            final TagsResponse tagsResponse = new TagsResponse(tags, metrics);
            response.resume(Response.status(Response.Status.OK)
                    .entity(tagsResponse).build());
        }
    }

    @Override
    public void queryTags(TagsQuery query, AsyncResponse response) {
        final List<Query<FindTagsResult>> queries = new ArrayList<Query<FindTagsResult>>();

        final Map<String, String> filter = query.getTags();
        final Set<String> namesFilter = query.getOnly();

        for (MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.findTags(filter, namesFilter));
            } catch (Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final GroupQuery<FindTagsResult> group = new GroupQuery<FindTagsResult>(
                queries);
        group.listen(new HandleFindTagsResult(response));
    }
}
