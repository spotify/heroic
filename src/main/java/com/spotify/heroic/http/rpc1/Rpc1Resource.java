package com.spotify.heroic.http.rpc1;

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.http.HttpAsyncUtils;
import com.spotify.heroic.http.rpc.RpcWriteResult;
import com.spotify.heroic.metrics.BackendGroup;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;

@Path("/rpc1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Rpc1Resource {
    @Inject
    private MetricBackendManager metrics;

    private static final HttpAsyncUtils.Resume<MetricGroups, MetricGroups> QUERY = new HttpAsyncUtils.Resume<MetricGroups, MetricGroups>() {
        @Override
        public MetricGroups resume(MetricGroups value) throws Exception {
            return value;
        }
    };

    @POST
    @Path("/query")
    public void query(@Suspended final AsyncResponse response,
            Rpc1QueryBody query) throws Exception {
        final Callback<MetricGroups> callback = metrics.useDefaultGroup()
                .groupedQuery(query.getKey().getTags(), null,
                        query.getSeries(), query.getRange(),
                        query.getAggregationGroup());

        HttpAsyncUtils.handleAsyncResume(response, callback, QUERY);
    }

    private static final HttpAsyncUtils.Resume<WriteBatchResult, RpcWriteResult> WRITE = new HttpAsyncUtils.Resume<WriteBatchResult, RpcWriteResult>() {
        @Override
        public RpcWriteResult resume(WriteBatchResult value) throws Exception {
            return new RpcWriteResult(value.isOk());
        }
    };

    @POST
    @Path("/write")
    public void write(@Suspended final AsyncResponse response,
            @QueryParam("backend") String backendGroup, List<WriteMetric> writes)
            throws Exception {
        final BackendGroup backend = metrics.useGroup(backendGroup);

        final Callback<WriteBatchResult> callback = metrics.write(
                backend, writes);

        HttpAsyncUtils.handleAsyncResume(response, callback, WRITE);
    }
}