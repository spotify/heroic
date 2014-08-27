package com.spotify.heroic.http.rpc;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.http.general.MessageResponse;

@Path("/rpc")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RpcResource {
    private final int VERSION = 1;

    @Inject
    private ClusterManager cluster;

    @GET
    @Path("/metadata")
    public Response getMetadata() {
        if (cluster == ClusterManager.NULL) {
            return Response
                    .status(Response.Status.NOT_IMPLEMENTED)
                    .entity(new MessageResponse(
                            "service is not configured as a cluster")).build();
        }

        final RpcMetadata metadata = new RpcMetadata(VERSION,
                cluster.getLocalNodeId(), cluster.getLocalNodeTags());
        return Response.status(Response.Status.OK).entity(metadata).build();
    }
}