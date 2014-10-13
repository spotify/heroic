package com.spotify.heroic.cluster.httprpc;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.spotify.heroic.cluster.model.NodeMetadata;

@Path("rpc")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HttpRpcResource {
    @Inject
    private NodeMetadata localMetadata;

    @GET
    @Path("metadata")
    public Response getMetadata() {
        final HttpRpcMetadata metadata = new HttpRpcMetadata(localMetadata.getVersion(), localMetadata.getId(),
                localMetadata.getTags(), localMetadata.getCapabilities());
        return Response.status(Response.Status.OK).entity(metadata).build();
    }
}