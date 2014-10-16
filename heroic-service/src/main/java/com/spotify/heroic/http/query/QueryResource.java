package com.spotify.heroic.http.query;

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

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.simple.AverageAggregation;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.metric.ClusteredMetricManager;
import com.spotify.heroic.metric.exceptions.MetricQueryException;
import com.spotify.heroic.metric.model.QueryMetricsResult;
import com.spotify.heroic.metric.model.ShardedMetricGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.utils.HttpAsyncUtils;

@Slf4j
@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {
    @Data
    private static final class MetricsResumer implements
            HttpAsyncUtils.Resume<QueryMetricsResult, QueryMetricsResponse> {
        private final Aggregation aggregation;
        private final DateRange range;

        @Override
        public QueryMetricsResponse resume(QueryMetricsResult result) throws Exception {
            final ShardedMetricGroups groups = result.getMetricGroups();
            return new QueryMetricsResponse(result.getQueryRange(), groups.getGroups(), groups.getStatistics(),
                    groups.getErrors());
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

        final Future<QueryMetricsResult> callback = metrics.query(q.getBackendGroup(), q.getFilter(), q.getGroupBy(),
                q.getRange(), q.getAggregation());

        response.setTimeout(300, TimeUnit.SECONDS);

        final Sampling sampling = q.getAggregation() != null ? q.getAggregation().getSampling() : null;
        final Aggregation aggregation = sampling != null ? new AverageAggregation(sampling) : null;

        HttpAsyncUtils.handleAsyncResume(response, callback, new MetricsResumer(aggregation, q.getRange()));
    }
}
