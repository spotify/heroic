package com.spotify.heroic.http;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.http.general.DataResponse;
import com.spotify.heroic.http.general.ErrorMessage;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricBackends;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.migrator.SeriesMigrator;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HeroicResource {
    @Inject
    private MetricManager metrics;

    @Inject
    private SeriesMigrator seriesMigrator;

    @POST
    @Path("/shutdown")
    public Response shutdown() {
        // lol, no :), send TERM signal instead.
        return Response.status(Response.Status.OK)
                .entity(new ErrorMessage("Not shutting down, use TERM signal instead.")).build();
    }

    @POST
    @Path("/migrate/{source}/{target}")
    public Response migrate(@PathParam("source") String sourceGroup, @PathParam("target") String targetGroup,
            @QueryParam("history") Long history, Filter filter) throws Exception {
        final MetricBackends source = metrics.useGroup(sourceGroup);
        final MetricBackends target = metrics.useGroup(targetGroup);

        if (source == null)
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("No such backend group: " + sourceGroup)).build();

        if (target == null)
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("No such backend group: " + targetGroup)).build();

        if (history == null)
            history = TimeUnit.MILLISECONDS.convert(365, TimeUnit.DAYS);

        try {
            seriesMigrator.migrate(history, source, target, filter);
        } catch (final Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorMessage(e.getMessage()))
                    .build();
        }

        return Response.status(Response.Status.OK).build();
    }

    @GET
    @Path("/backends")
    public Response getBackends() {
        final List<String> results = new ArrayList<>();

        for (final MetricBackend b : metrics.getBackends()) {
            results.add(b.getGroup());
        }

        return Response.status(Response.Status.OK).entity(new DataResponse<>(results)).build();
    }
}
