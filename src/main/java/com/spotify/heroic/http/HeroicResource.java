package com.spotify.heroic.http;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.http.general.DataResponse;
import com.spotify.heroic.http.general.ErrorMessage;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.model.BackendEntry;
import com.spotify.heroic.model.WriteMetric;

@Slf4j
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HeroicResource {
    @Inject
    private MetricBackendManager metrics;

    @POST
    @Path("/shutdown")
    public Response shutdown() {
        // lol, no :), send TERM signal instead.
        return Response.status(Response.Status.OK)
                .entity(new ErrorMessage("shutting down")).build();
    }

    @GET
    @Path("/migrate/{from}/{to}")
    public Response migrate(@PathParam("from") String from,
            @PathParam("to") String to) {
        final Backend source = metrics.getBackend(from);
        final Backend target = metrics.getBackend(to);

        if (source == null)
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("No such backend: " + from))
                    .build();

        if (target == null)
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("No such backend: " + to)).build();

        for (final BackendEntry entry : source.listEntries()) {
            log.info("{}: {} datapoint(s)", entry.getSeries(), entry
                    .getDataPoints().size());

            try {
                target.write(
                        new WriteMetric(entry.getSeries(), entry
                                .getDataPoints())).get();
            } catch (final Exception e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new ErrorMessage(e.getMessage())).build();
            }
        }

        return Response.status(Response.Status.OK).build();
    }

    @GET
    @Path("/backends")
    public Response getBackends() {
        final List<String> results = new ArrayList<>();

        for (final Backend b : metrics.getBackends()) {
            results.add(b.getId());
        }

        return Response.status(Response.Status.OK)
                .entity(new DataResponse<>(results)).build();
    }
}
