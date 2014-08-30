package com.spotify.heroic.http;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;
import javax.servlet.ServletResponse;
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
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.http.general.DataResponse;
import com.spotify.heroic.http.general.ErrorMessage;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.BackendGroup;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.model.FetchData;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.filter.Filter;

@Slf4j
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HeroicResource {
    @Inject
    private MetricBackendManager metrics;

    @Inject
    private MetadataBackendManager metadata;

    @Inject
    private ClusterManager cluster;

    @Inject
    private ExecutorService executor;

    @POST
    @Path("/shutdown")
    public Response shutdown() {
        // lol, no :), send TERM signal instead.
        return Response.status(Response.Status.OK)
                .entity(new ErrorMessage("shutting down")).build();
    }

    @POST
    @Path("/migrate/{source}/{target}")
    public Response migrate(@PathParam("source") String source,
            @PathParam("target") String target,
            @QueryParam("history") Long history, ServletResponse response,
            Filter filter) throws Exception {
        if (cluster == ClusterManager.NULL)
            return Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage(
                            "Cannot migrate since instance is not configured as a cluster"))
                            .build();

        final BackendGroup sourceCluster = metrics.useGroup(source);
        final BackendGroup targetCluster = metrics.useGroup(target);

        if (sourceCluster == null)
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("No such backend group: " + source))
                    .build();

        if (targetCluster == null)
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("No such backend group: " + target))
                    .build();

        if (history == null)
            history = TimeUnit.MILLISECONDS.convert(365, TimeUnit.DAYS);

        final Set<Series> series;

        try {
            series = metadata.findTimeSeries(filter).get().getSeries();
        } catch (final Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorMessage(e.getMessage())).build();
        }

        try {
            migrateData(history, sourceCluster, targetCluster, series);
        } catch (final Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorMessage(e.getMessage())).build();
        }

        return Response.status(Response.Status.OK).build();
    }

    private void migrateData(long history, final BackendGroup source,
            final BackendGroup target, final Set<Series> series)
                    throws Exception {
        final long now = System.currentTimeMillis();

        final DateRange range = new DateRange(now - history, now);

        final String session = Integer.toHexString(new Object().hashCode());
        final Semaphore available = new Semaphore(50, true);
        final AtomicInteger count = new AtomicInteger(0);

        final int total = series.size();

        for (final Series s : series) {
            available.acquire();

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    final int id = count.incrementAndGet();

                    final String threadSession = String.format("%s-%04d/%04d",
                            session, id, total);

                    try {
                        migrate(threadSession, source, target, series, range, s);
                    } catch (final Exception e) {
                        log.error(String.format("%s: Migrate of %s failed",
                                session, s), e);
                    }

                    available.release();
                }
            });
        }
    }

    private void migrate(final String session, final BackendGroup source,
            final BackendGroup target, final Set<Series> series,
            final DateRange range, final Series s) throws Exception {
        final Callback.Reducer<FetchData, List<DataPoint>> reducer = new Callback.Reducer<FetchData, List<DataPoint>>() {
            @Override
            public List<DataPoint> resolved(Collection<FetchData> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception e : errors)
                    log.error("{}: Failed to read entry", session, e);

                for (final CancelReason reason : cancelled)
                    log.error("{}, Entry read cancelled: {}", session, reason);

                if (errors.size() > 0 || cancelled.size() > 0)
                    throw new Exception("Errors during read");

                final List<DataPoint> datapoints = new ArrayList<>();

                for (final FetchData r : results)
                    datapoints.addAll(r.getDatapoints());

                Collections.sort(datapoints);

                return datapoints;
            }
        };

        final List<DataPoint> datapoints = ConcurrentCallback.newReduce(
                source.query(s, range), reducer).get();

        log.info(String.format("%s: Writing %d datapoint(s) for series: %s",
                session, datapoints.size(), s));

        target.write(
                Arrays.asList(new WriteMetric[] { new WriteMetric(s, datapoints) }))
                .get();
    }

    @GET
    @Path("/backends")
    public Response getBackends() {
        final List<String> results = new ArrayList<>();

        for (final Backend b : metrics.getBackends()) {
            results.add(b.getGroup());
        }

        return Response.status(Response.Status.OK)
                .entity(new DataResponse<>(results)).build();
    }
}
