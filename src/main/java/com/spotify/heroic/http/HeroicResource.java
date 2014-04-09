package com.spotify.heroic.http;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.ConnectionCallback;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.BackendManager.DataPointGroup;
import com.spotify.heroic.backend.BackendManager.QueryMetricsResult;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.backend.TimeSeriesCache;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.KeysResponse;
import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.query.MetricsResponse;
import com.spotify.heroic.query.TagsQuery;
import com.spotify.heroic.query.TagsResponse;
import com.spotify.heroic.query.TimeSeriesQuery;
import com.spotify.heroic.query.TimeSeriesResponse;

@Slf4j
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class HeroicResource {
    @Inject
    private BackendManager backendManager;

    @Inject
    private TimeSeriesCache timeSeriesCache;

    @Inject
    private HeroicResourceCache cache;

    public static final class Message {
        @Getter
        private final String message;

        public Message(String message) {
            this.message = message;
        }
    }

    @POST
    @Path("/shutdown")
    public Response shutdown() {
        return Response.status(Response.Status.OK)
                .entity(new Message("shutting down")).build();
    }

    @POST
    @Path("/metrics")
    @Consumes(MediaType.APPLICATION_JSON)
    public void metrics(@Suspended final AsyncResponse response,
            MetricsQuery query) throws QueryException {
        log.info("Query: " + query);

        final Callback<QueryMetricsResult> callback = backendManager
                .queryMetrics(query).register(
                        new Callback.Handle<QueryMetricsResult>() {
                            @Override
                            public void cancel(CancelReason reason)
                                    throws Exception {
                                response.resume(Response
                                        .status(Response.Status.GATEWAY_TIMEOUT)
                                        .entity(new ErrorMessage(
                                                "Request cancelled: " + reason))
                                        .build());
                            }

                            @Override
                            public void error(Throwable e) throws Exception {
                                response.resume(Response
                                        .status(Response.Status.INTERNAL_SERVER_ERROR)
                                        .entity(e).build());
                            }

                            @Override
                            public void finish(QueryMetricsResult result)
                                    throws Exception {
                                final Map<Map<String, String>, List<DataPoint>> data = makeData(result
                                        .getGroups());

                                final MetricsResponse entity = new MetricsResponse(
                                        data, result.getSampleSize(), result
                                                .getOutOfBounds(), result
                                                .getRowStatistics());

                                response.resume(Response
                                        .status(Response.Status.OK)
                                        .entity(entity).build());
                            }

                            private Map<Map<String, String>, List<DataPoint>> makeData(
                                    List<DataPointGroup> groups) {
                                final Map<Map<String, String>, List<DataPoint>> data = new HashMap<Map<String, String>, List<DataPoint>>();

                                for (final DataPointGroup group : groups) {
                                    data.put(group.getTags(),
                                            group.getDatapoints());
                                }

                                return data;
                            }
                        });

        response.setTimeout(300, TimeUnit.SECONDS);

        response.setTimeoutHandler(new TimeoutHandler() {
            @Override
            public void handleTimeout(AsyncResponse asyncResponse) {
                log.info("Request timed out");
                callback.cancel(new CancelReason("Request timed out"));
            }
        });

        response.register(new CompletionCallback() {
            @Override
            public void onComplete(Throwable throwable) {
                log.info("Client completed");
                callback.cancel(new CancelReason("Client completed"));
            }
        });

        response.register(new ConnectionCallback() {
            @Override
            public void onDisconnect(AsyncResponse disconnected) {
                log.info("Client disconnected");
                callback.cancel(new CancelReason("Client disconnected"));
            }
        });
    }

    @POST
    @Path("/tags")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response tags(TagsQuery query) {
        if (!cache.isReady()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build();
        }

        final TagsResponse response = cache.tags(query);
        return Response.status(Response.Status.OK).entity(response).build();
    }

    @POST
    @Path("/keys")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response keys(TimeSeriesQuery query) {
        if (!cache.isReady()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build();
        }

        final KeysResponse response = cache.keys(query);
        return Response.status(Response.Status.OK).entity(response).build();
    }

    @POST
    @Path("/timeseries")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response timeseries(TimeSeriesQuery query) {
        if (!timeSeriesCache.isReady()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build();
        }

        final TimeSeriesResponse response = cache.timeseries(query);
        return Response.status(Response.Status.OK).entity(response).build();
    }
}
