package com.spotify.heroic.http.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.http.HttpAsyncUtils;
import com.spotify.heroic.metric.ClusteredMetricManager;
import com.spotify.heroic.metric.MetricQueryException;
import com.spotify.heroic.metric.model.MetricGroup;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.QueryMetricsResult;
import com.spotify.heroic.model.DataPoint;

@Slf4j
@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {
    private static final HttpAsyncUtils.Resume<QueryMetricsResult, QueryMetricsResponse> WRITE_METRICS = new HttpAsyncUtils.Resume<QueryMetricsResult, QueryMetricsResponse>() {
        @Override
        public QueryMetricsResponse resume(QueryMetricsResult result) throws Exception {
            final MetricGroups groups = result.getMetricGroups();
            final Map<Map<String, String>, List<DataPoint>> data = makeData(groups.getGroups());
            return new QueryMetricsResponse(result.getQueryRange(), data, groups.getStatistics(), groups.getErrors());
        }

        private Map<Map<String, String>, List<DataPoint>> makeData(List<MetricGroup> groups) {
            final Map<Map<String, String>, List<DataPoint>> data = new HashMap<>();

            for (final MetricGroup group : groups) {
                data.put(group.getGroup(), group.getDatapoints());
            }

            return data;
        }
    };

    @Inject
    private ClusteredMetricManager metrics;

    @POST
    @Path("/metrics")
    public void metrics(@Suspended final AsyncResponse response, @QueryParam("backend") String backendGroup,
            QueryMetrics query) throws MetricQueryException {
        final QueryPrepared q = QueryPrepared.create(backendGroup, query);

        log.info("Metrics: {}", q);

        final Callback<QueryMetricsResult> callback = metrics.queryMetrics(q.getBackendGroup(), q.getFilter(),
                q.getGroupBy(), q.getRange(), q.getAggregation());

        response.setTimeout(300, TimeUnit.SECONDS);

        HttpAsyncUtils.handleAsyncResume(response, callback, WRITE_METRICS);
    }
}
