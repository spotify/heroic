package com.spotify.heroic.http;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.http.general.DataResponse;
import com.spotify.heroic.http.general.ErrorMessage;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteMetric;

@Slf4j
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HeroicResource {
    @Inject
    private MetricBackendManager metrics;

    @Inject
    private MetadataBackendManager metadata;

    @POST
    @Path("/shutdown")
    public Response shutdown() {
        // lol, no :), send TERM signal instead.
        return Response.status(Response.Status.OK)
                .entity(new ErrorMessage("shutting down")).build();
    }

    @POST
    @Path("/migrate/{from}/{to}")
    public Response migrate(@PathParam("from") String from,
            @PathParam("to") String to, @QueryParam("history") Long history) {
        if (history == null)
            history = TimeUnit.MILLISECONDS.convert(365, TimeUnit.DAYS);

        final Backend source = metrics.getBackend(from);
        final Backend target = metrics.getBackend(to);

        if (source == null)
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("No such backend: " + from))
                    .build();

        if (target == null)
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("No such backend: " + to)).build();

        final Set<Series> series;

        try {
            series = metadata.findTimeSeries(null).get().getSeries();
        } catch (final Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorMessage(e.getMessage())).build();
        }

        try {
            migrateData(history, source, target, series);
        } catch (final Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorMessage(e.getMessage())).build();
        }

        return Response.status(Response.Status.OK).build();
    }

    private void migrateData(long history, final Backend source,
            final Backend target, Set<Series> series) throws Exception {
        final long now = System.currentTimeMillis();

        final DateRange range = new DateRange(now - history, now);

        for (final Series s : series) {
            final Callback.Reducer<FetchDataPoints.Result, List<DataPoint>> reducer = new Callback.Reducer<FetchDataPoints.Result, List<DataPoint>>() {
                @Override
                public List<DataPoint> resolved(
                        Collection<FetchDataPoints.Result> results,
                        Collection<Exception> errors,
                        Collection<CancelReason> cancelled) throws Exception {
                    for (final Exception e : errors)
                        log.error("Failed to read entry", e);

                    for (final CancelReason reason : cancelled)
                        log.error("Entry read cancelled: " + reason);

                    if (errors.size() > 0 || cancelled.size() > 0)
                        throw new Exception("Errors during read");

                    final List<DataPoint> datapoints = new ArrayList<>();

                    for (final FetchDataPoints.Result r : results) {
                        datapoints.addAll(r.getDatapoints());
                    }

                    Collections.sort(datapoints);

                    return datapoints;
                }
            };

            try {
                final List<DataPoint> datapoints = ConcurrentCallback
                        .newReduce(source.query(s, range), reducer).get();

                log.info("Writing {} datapoint(s) for series: ",
                        datapoints.size(), s);

                target.write(new WriteMetric(s, datapoints)).get();
            } catch (final Exception e) {
                throw new Exception("Migrate for " + series + " failed", e);
            }
        }
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
