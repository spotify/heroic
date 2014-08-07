package com.spotify.heroic.http;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.http.model.ClusterMetadataResponse;

@Slf4j
@Path("/rpc")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RPCResource {
    @Inject
    private ClusterManager cluster;

    @GET
    @Path("/metadata")
    public Response getMetadata() {
        final ClusterMetadataResponse metadata = new ClusterMetadataResponse(
                cluster.getLocalNodeId(), cluster.getLocalNodeTags());
        return Response.status(Response.Status.OK).entity(metadata).build();
    }
}