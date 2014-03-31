package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.backend.MetricBackend.DataPointsQuery;
import com.spotify.heroic.backend.MetricBackend.DataPointsResult;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.MetricsQuery;

@Slf4j
public class ListBackendManager implements BackendManager {
    private final ExecutorService executorService;

    @Getter
    private final List<MetricBackend> metricBackends;

    @Getter
    private final List<EventBackend> eventBackends;

    @Getter
    private final long timeout;

    public ListBackendManager(List<Backend> backends, long timeout,
            int threadPoolSize) {
        this.executorService = Executors.newScheduledThreadPool(threadPoolSize);
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

    @Override
    public void queryMetrics(final MetricsQuery query,
            final AsyncResponse response) {
        final List<Query<MetricBackend.DataPointsQuery>> queries = new ArrayList<Query<MetricBackend.DataPointsQuery>>();

        for (MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.query(query));
            } catch (Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        final GroupQuery<MetricBackend.DataPointsQuery> group = new GroupQuery<MetricBackend.DataPointsQuery>(
                queries);

        group.listen(new GroupQuery.Handle<MetricBackend.DataPointsQuery>() {
            @Override
            public void done(Collection<DataPointsQuery> results,
                    Collection<Throwable> errors, int cancelled)
                    throws Exception {
                /*
                 * if (response.isCancelled()) { log.info(
                 * "Not continuing request since response has been cancelled");
                 * return; }
                 */

                log.info("results: " + results);
                log.info("errors: " + errors);
                log.info("cancelled: " + cancelled);
                log.info("response: " + response.isCancelled());

                for (final DataPointsQuery engine : results) {
                    if (engine.isEmpty())
                        continue;

                    final GroupQuery<DataPointsResult> group = engine
                            .query(query);

                    group.listen(new GroupQuery.Handle<DataPointsResult>() {
                        @Override
                        public void done(Collection<DataPointsResult> results,
                                Collection<Throwable> errors, int cancelled)
                                throws Exception {
                            for (Throwable error : errors) {
                                log.error("Failed: " + errors.size(), error);
                            }

                            log.info("Cancelled: " + cancelled);

                            List<DataPoint> datapoints = new ArrayList<DataPoint>();

                            for (DataPointsResult result : results) {
                                log.info("Row queried: " + result.getRowKey());
                                datapoints.addAll(result.getDatapoints());
                            }

                            Collections.sort(datapoints);
                            log.info("size = " + datapoints.size());
                            response.resume(Response.status(Response.Status.OK)
                                    .entity("Everything OK").build());
                        }
                    });
                }
            }
        });
    }
}
