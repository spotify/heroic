package com.spotify.heroic.http.rpc;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.spotify.heroic.cluster.ClusterManager;

@Path("/rpc")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RpcResource {
    public static final int VERSION = 3;

    @Inject
    private ClusterManager cluster;

    @GET
    @Path("/metadata")
    public Response getMetadata() {
        final RpcMetadata metadata = new RpcMetadata(VERSION,
                cluster.getLocalNodeId(), cluster.getLocalNodeTags(),
                cluster.getCapabilities());
        return Response.status(Response.Status.OK).entity(metadata).build();
    }
}