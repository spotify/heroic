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
import com.spotify.heroic.metrics.BackendCluster;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.WriteResult;

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
        final Callback<MetricGroups> callback = metrics.directQuery(
                query.getBackendGroup(), query.getKey(), query.getSeries(),
                query.getRange(), query.getAggregationGroup());

        HttpAsyncUtils.handleAsyncResume(response, callback, QUERY);
    }

    private static final HttpAsyncUtils.Resume<WriteResult, RpcWriteResult> WRITE = new HttpAsyncUtils.Resume<WriteResult, RpcWriteResult>() {
        @Override
        public RpcWriteResult resume(WriteResult value) throws Exception {
            final boolean ok = value.getFailed().size()
                    + value.getCancelled().size() == 0;
            return new RpcWriteResult(ok);
        }
    };

    @POST
    @Path("/write")
    public void write(@Suspended final AsyncResponse response,
            @QueryParam("backend") String backendGroup, List<WriteMetric> writes)
            throws Exception {
        final BackendCluster backend = metrics.with(backendGroup);

        final Callback<WriteResult> callback = metrics.writeDirect(backend,
                writes);

        HttpAsyncUtils.handleAsyncResume(response, callback, WRITE);
    }
}