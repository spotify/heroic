package com.spotify.heroic.http.utils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import com.google.inject.Inject;
import com.spotify.heroic.HeroicInternalLifeCycle;

@Path("utils")
public class UtilsResource {
    @Inject
    private HeroicInternalLifeCycle lifecycle;

    @GET
    @Path("wait")
    public void wait(@Suspended final AsyncResponse response) {
        lifecycle.register("client wait", new HeroicInternalLifeCycle.StartupHook() {
            @Override
            public void onStartup(HeroicInternalLifeCycle.Context context) throws Exception {
                response.resume(Response.status(Response.Status.OK).entity("started").build());
            }
        });
    }
}
