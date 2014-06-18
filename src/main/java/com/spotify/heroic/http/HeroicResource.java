package com.spotify.heroic.http;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.ConnectionCallback;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseFeature;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.BackendManager.StreamMetricsResult;
import com.spotify.heroic.backend.model.MetricGroup;
import com.spotify.heroic.backend.model.MetricGroups;
import com.spotify.heroic.backend.MetricStream;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.backend.TimeSeriesCache;
import com.spotify.heroic.http.model.KeysResponse;
import com.spotify.heroic.http.model.MetricsRequest;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.http.model.MetricsResponse;
import com.spotify.heroic.http.model.MetricsStreamResponse;
import com.spotify.heroic.http.model.TagsRequest;
import com.spotify.heroic.http.model.TagsResponse;
import com.spotify.heroic.http.model.TimeSeriesRequest;
import com.spotify.heroic.http.model.TimeSeriesResponse;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

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

    @Inject
    private StoredMetricsQueries storedQueries;

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
    @Path("/metrics-stream")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response makeMetricsStream(MetricsRequest query, @Context UriInfo info) {
        final String id = Integer.toHexString(query.hashCode());
        storedQueries.put(id, query);
        final URI location = info.getBaseUriBuilder().path("/metrics-stream/" + id).build();
        final MetricsStreamResponse entity = new MetricsStreamResponse(id);
        return Response.created(location).entity(entity).build();
    }

    @GET
    @Path("/metrics-stream/{id}")
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    public EventOutput getMetricsStream(@PathParam("id") String id) throws WebApplicationException, QueryException {
        final MetricsRequest query = storedQueries.get(id);

        if (query == null) {
            throw new NotFoundException("No such stored query: " + id);
        }

        log.info("Query: {}", query);

        final EventOutput eventOutput = new EventOutput();

        final Callback<StreamMetricsResult> callback = backendManager.streamMetrics(query, new MetricStream() {
            public void stream(Callback<StreamMetricsResult> callback, MetricsQueryResponse result) throws Exception {
                if (eventOutput.isClosed()) {
                    callback.cancel(new CancelReason("client disconnected"));
                    return;
                }

                final MetricGroups groups = result.getMetricGroups();
                final Map<TimeSerie, List<DataPoint>> data = makeData(groups.getGroups());
                final MetricsResponse entity = new MetricsResponse(
                        result.getQueryRange(), data, groups.getStatistics());
                final OutboundEvent.Builder builder = new OutboundEvent.Builder();

                builder.mediaType(MediaType.APPLICATION_JSON_TYPE);
                builder.name("metrics");
                builder.data(MetricsResponse.class, entity);
                eventOutput.write(builder.build());
            }
        });

        callback.register(new Callback.Handle<StreamMetricsResult>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                sendEvent(eventOutput, "cancel", new ErrorMessage(reason.getMessage()));
            }

            @Override
            public void failed(Exception e) throws Exception {
                sendEvent(eventOutput, "error", new ErrorMessage(e.getMessage()));
            }

            @Override
            public void resolved(StreamMetricsResult result) throws Exception {
                sendEvent(eventOutput, "end", "end");
            }

            private void sendEvent(final EventOutput eventOutput, String type, Object message)
                    throws IOException {
                final OutboundEvent.Builder builder = new OutboundEvent.Builder();

                builder.mediaType(MediaType.APPLICATION_JSON_TYPE);
                builder.name(type);
                builder.data(message);

                final OutboundEvent event = builder.build();

                eventOutput.write(event);
                eventOutput.close();
            }
        });

        return eventOutput;
    }

    @POST
    @Path("/metrics")
    @Consumes(MediaType.APPLICATION_JSON)
    public void metrics(@Suspended final AsyncResponse response,
            MetricsRequest query) throws QueryException {
        log.info("Query: " + query);

        final Callback<MetricsQueryResponse> callback = backendManager.queryMetrics(query).register(
                new Callback.Handle<MetricsQueryResponse>() {
                    @Override
                    public void cancelled(CancelReason reason)
                            throws Exception {
                        response.resume(Response
                                .status(Response.Status.GATEWAY_TIMEOUT)
                                .entity(new ErrorMessage(
                                        "Request cancelled: " + reason))
                                        .build());
                    }

                    @Override
                    public void failed(Exception e) throws Exception {
                        response.resume(Response
                                .status(Response.Status.INTERNAL_SERVER_ERROR)
                                .entity(e).build());
                    }

                    @Override
                    public void resolved(MetricsQueryResponse result)
                            throws Exception {
                        final MetricGroups groups = result.getMetricGroups();
                        final Map<TimeSerie, List<DataPoint>> data = makeData(groups.getGroups());
                        final MetricsResponse entity = new MetricsResponse(
                                result.getQueryRange(), data, groups.getStatistics());

                        response.resume(Response
                                .status(Response.Status.OK)
                                .entity(entity).build());
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
    public Response tags(TagsRequest query) {
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
    public Response keys(TimeSeriesRequest query) {
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
    public Response getTimeSeries(TimeSeriesRequest query) {
        if (!timeSeriesCache.isReady()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build();
        }

        final TimeSeriesResponse response = cache.timeseries(query);
        return Response.status(Response.Status.OK).entity(response).build();
    }

    private static Map<TimeSerie, List<DataPoint>> makeData(
            List<MetricGroup> groups) {
        final Map<TimeSerie, List<DataPoint>> data = new HashMap<TimeSerie, List<DataPoint>>();

        for (final MetricGroup group : groups) {
            data.put(group.getTimeSerie(), group.getDatapoints());
        }

        return data;
    }
}
