package com.spotify.heroic.cluster.httprpc.rpc4;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.spotify.heroic.async.Future;
import com.spotify.heroic.cluster.httprpc.model.RpcWriteResult;
import com.spotify.heroic.metric.MetricBackends;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.utils.HttpAsyncUtils;

@Path("rpc4")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Rpc4Resource {
    @Inject
    private MetricManager metrics;

    private static final HttpAsyncUtils.Resume<MetricGroups, Rpc4MetricGroups> QUERY = new HttpAsyncUtils.Resume<MetricGroups, Rpc4MetricGroups>() {
        @Override
        public Rpc4MetricGroups resume(MetricGroups value) throws Exception {
            return new Rpc4MetricGroups(value.getGroups(), value.getStatistics(), value.getErrors());
        }
    };

    private static final HttpAsyncUtils.Resume<WriteBatchResult, RpcWriteResult> WRITE = new HttpAsyncUtils.Resume<WriteBatchResult, RpcWriteResult>() {
        @Override
        public RpcWriteResult resume(WriteBatchResult value) throws Exception {
            return new RpcWriteResult(value.isOk());
        }
    };

    @POST
    @Path("query")
    public void query(@Suspended final AsyncResponse response, Rpc4QueryBody query) throws Exception {
        final Future<MetricGroups> callback = metrics.useGroup(query.getBackendGroup()).query(query.getGroup(),
                query.getFilter(), query.getSeries(), query.getRange(), query.getAggregationGroup());

        HttpAsyncUtils.handleAsyncResume(response, callback, QUERY);
    }

    @POST
    @Path("write")
    public void write(@Suspended final AsyncResponse response, Rpc4WriteBody body) throws Exception {
        final MetricBackends backend = metrics.useGroup(body.getBackendGroup());

        final Future<WriteBatchResult> callback = backend.write(body.getWrites());

        HttpAsyncUtils.handleAsyncResume(response, callback, WRITE);
    }

    @POST
    @Path("full-query")
    public void query(@Suspended final AsyncResponse response, Rpc4FullQueryBody body) throws Exception {
        final Future<MetricGroups> callback = metrics.queryMetrics(body.getBackendGroup(), body.getFilter(),
                body.getGroupBy(), body.getRange(), body.getAggregation());
        HttpAsyncUtils.handleAsyncResume(response, callback, QUERY);
    }
}