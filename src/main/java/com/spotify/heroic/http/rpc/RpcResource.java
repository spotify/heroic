package com.spotify.heroic.http.rpc;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.http.rpc.model.ClusterMetadataResponse;
import com.spotify.heroic.http.rpc.model.RpcQueryRequest;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.model.MetricGroups;

@Slf4j
@Path("/rpc")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RpcResource {
    @Inject
    private ClusterManager cluster;

    @Inject
    private MetricBackendManager metrics;

    @GET
    @Path("/metadata")
    public Response getMetadata() {
        final ClusterMetadataResponse metadata = new ClusterMetadataResponse(
                cluster.getLocalNodeId(), cluster.getLocalNodeTags());
        return Response.status(Response.Status.OK).entity(metadata).build();
    }

    @POST
    @Path("/query")
    public void query(@Suspended final AsyncResponse response,
            RpcQueryRequest query) {
        metrics.rpcQueryMetrics(query).register(
                new Callback.Handle<MetricGroups>() {
                    @Override
                    public void cancelled(CancelReason reason) throws Exception {
                    }

                    @Override
                    public void failed(Exception e) throws Exception {
                    }

                    @Override
                    public void resolved(MetricGroups result) throws Exception {
                        response.resume(Response.status(Response.Status.OK)
                                .entity(result).build());
                    }
                });
    }
}