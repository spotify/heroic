package com.spotify.heroic.http;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.inject.Inject;
import com.spotify.heroic.httpclient.model.DataResponse;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.utils.GroupMember;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HeroicResource {
    @Inject
    private MetricManager metrics;

    @Inject
    private MetadataManager metadata;

    @Inject
    private SuggestManager suggest;

    @POST
    @Path("/shutdown")
    public Response shutdown() {
        // lol, no :), send TERM signal instead.
        return Response.status(Response.Status.OK)
                .entity(new ErrorMessage("Not shutting down, use TERM signal instead.")).build();
    }

    @GET
    @Path("/backends")
    public Response getBackends() {
        final List<String> results = new ArrayList<>();

        for (final GroupMember<MetricBackend> b : metrics.getBackends()) {
            results.add(b.toString());
        }

        for (final GroupMember<MetadataBackend> b : metadata.getBackends()) {
            results.add(b.toString());
        }

        for (final GroupMember<SuggestBackend> b : suggest.getBackends()) {
            results.add(b.toString());
        }

        return Response.status(Response.Status.OK).entity(new DataResponse<>(results)).build();
    }
}
