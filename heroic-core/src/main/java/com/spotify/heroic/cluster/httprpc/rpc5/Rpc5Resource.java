package com.spotify.heroic.cluster.httprpc.rpc5;

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
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.utils.HttpAsyncUtils;

@Path("rpc5")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Rpc5Resource {
    @Inject
    private MetricManager localMetrics;

    @Inject
    private MetadataManager localMetadata;

    private static final HttpAsyncUtils.Resume<MetricGroups, Rpc5MetricGroups> QUERY = new HttpAsyncUtils.Resume<MetricGroups, Rpc5MetricGroups>() {
        @Override
        public Rpc5MetricGroups resume(MetricGroups value) throws Exception {
            return new Rpc5MetricGroups(value.getGroups(), value.getStatistics(), value.getErrors());
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
    public void query(@Suspended final AsyncResponse response, Rpc5QueryBody query) throws Exception {
        final Future<MetricGroups> callback = localMetrics.useGroup(query.getBackendGroup()).query(query.getGroup(),
                query.getFilter(), query.getSeries(), query.getRange(), query.getAggregationGroup());

        HttpAsyncUtils.handleAsyncResume(response, callback, QUERY);
    }

    @POST
    @Path("write")
    public void write(@Suspended final AsyncResponse response, Rpc5WriteBody body) throws Exception {
        final Future<WriteBatchResult> callback = localMetrics.useGroup(body.getBackendGroup()).write(body.getWrites());
        HttpAsyncUtils.handleAsyncResume(response, callback, WRITE);
    }

    @POST
    @Path("full-query")
    public void query(@Suspended final AsyncResponse response, Rpc5FullQueryBody body) throws Exception {
        final Future<MetricGroups> callback = localMetrics.queryMetrics(body.getBackendGroup(), body.getFilter(),
                body.getGroupBy(), body.getRange(), body.getAggregation());
        HttpAsyncUtils.handleAsyncResume(response, callback, QUERY);
    }

    @POST
    @Path("find-tags")
    public void findTags(@Suspended final AsyncResponse response, Filter filter) {
        HttpAsyncUtils.handleAsyncResume(response, localMetadata.findTags(filter));
    }

    @POST
    @Path("find-keys")
    public void findKeys(@Suspended final AsyncResponse response, Filter filter) {
        HttpAsyncUtils.handleAsyncResume(response, localMetadata.findKeys(filter));
    }

    @POST
    @Path("find-series")
    public void findSeries(@Suspended final AsyncResponse response, Filter filter) {
        HttpAsyncUtils.handleAsyncResume(response, localMetadata.findSeries(filter));
    }

    @POST
    @Path("delete-series")
    public void deleteSeries(@Suspended final AsyncResponse response, Filter filter) {
        HttpAsyncUtils.handleAsyncResume(response, localMetadata.deleteSeries(filter));
    }

    @POST
    @Path("write-series")
    public void writeSeries(@Suspended final AsyncResponse response, Series series) {
        HttpAsyncUtils.handleAsyncResume(response, localMetadata.bufferWrite(series));
    }
}