package com.spotify.heroic.http;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.spotify.heroic.http.model.MessageResponse;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HeroicResource {
    @POST
    @Path("/shutdown")
    public Response shutdown() {
        // lol, no :)
        return Response.status(Response.Status.OK)
                .entity(new MessageResponse("shutting down")).build();
    }
}
