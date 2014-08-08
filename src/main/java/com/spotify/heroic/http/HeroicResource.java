package com.spotify.heroic.http;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

import com.google.common.io.BaseEncoding;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.http.model.DecodeRowKeyRequest;
import com.spotify.heroic.http.model.DecodeRowKeyResponse;
import com.spotify.heroic.http.model.EncodeRowKeyRequest;
import com.spotify.heroic.http.model.EncodeRowKeyResponse;
import com.spotify.heroic.http.model.KeysResponse;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.http.model.MetricsRequest;
import com.spotify.heroic.http.model.MetricsResponse;
import com.spotify.heroic.http.model.MetricsStreamResponse;
import com.spotify.heroic.http.model.TagsRequest;
import com.spotify.heroic.http.model.TagsResponse;
import com.spotify.heroic.http.model.TimeSeriesRequest;
import com.spotify.heroic.http.model.TimeSeriesResponse;
import com.spotify.heroic.http.model.WriteMetricsRequest;
import com.spotify.heroic.http.model.WriteMetricsResponse;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.MetadataQueryException;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.MetricQueryException;
import com.spotify.heroic.metrics.MetricStream;
import com.spotify.heroic.metrics.heroic.MetricsRowKey;
import com.spotify.heroic.metrics.heroic.MetricsRowKeySerializer;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.StreamMetricsResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteEntry;
import com.spotify.heroic.model.WriteResponse;

@Slf4j
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HeroicResource {
    @Inject
    private MetricBackendManager metrics;

    @Inject
    private MetadataBackendManager metadataBackend;

    @Inject
    private StoredMetricQueries storedQueries;

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
    @Produces(MediaType.APPLICATION_JSON)
    public Response makeMetricsStream(MetricsRequest query,
            @Context UriInfo info) {
        log.info("POST /metrics-stream: {}", query);

        final String id = Integer.toHexString(query.hashCode());
        storedQueries.put(id, query);
        final URI location = info.getBaseUriBuilder()
                .path("/metrics-stream/" + id).build();
        final MetricsStreamResponse entity = new MetricsStreamResponse(id);
        return Response.created(location).entity(entity).build();
    }

    @GET
    @Path("/metrics-stream/{id}")
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    public EventOutput getMetricsStream(@PathParam("id") String id)
            throws WebApplicationException, MetricQueryException {
        log.info("GET /metrics-stream/{}", id);

        final MetricsRequest query = storedQueries.get(id);

        if (query == null) {
            throw new NotFoundException("No such stored query: " + id);
        }

        log.info("Query: {}", query);

        final EventOutput eventOutput = new EventOutput();

        final Callback<StreamMetricsResult> callback = metrics.streamMetrics(
                query, new MetricStream() {
                    @Override
                    public void stream(Callback<StreamMetricsResult> callback,
                            MetricsQueryResponse result) throws Exception {
                        if (eventOutput.isClosed()) {
                            callback.cancel(new CancelReason(
                                    "client disconnected"));
                            return;
                        }

                        final MetricGroups groups = result.getMetricGroups();
                        final Map<TimeSerie, List<DataPoint>> data = makeData(groups
                                .getGroups());
                        final MetricsResponse entity = new MetricsResponse(
                                result.getQueryRange(), data, groups
                                .getStatistics());
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
                sendEvent(eventOutput, "cancel",
                        new ErrorMessage(reason.getMessage()));
            }

            @Override
            public void failed(Exception e) throws Exception {
                sendEvent(eventOutput, "error",
                        new ErrorMessage(e.getMessage()));
            }

            @Override
            public void resolved(StreamMetricsResult result) throws Exception {
                sendEvent(eventOutput, "end", "end");
            }

            private void sendEvent(final EventOutput eventOutput, String type,
                    Object message) throws IOException {
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

    interface Resume<T, R> {
        public R resume(T value) throws Exception;
    }

    private static final Resume<WriteResponse, WriteMetricsResponse> METRICS = new Resume<WriteResponse, WriteMetricsResponse>() {
        @Override
        public WriteMetricsResponse resume(WriteResponse result)
                throws Exception {
            return new WriteMetricsResponse(0);
        }
    };

    @POST
    @Path("/write")
    public void writeMetrics(@Suspended final AsyncResponse response,
            WriteMetricsRequest write) {
        log.info("POST /write: {}", write);

        final WriteEntry entry = new WriteEntry(
                write.getTimeSerie(), write.getData());

        final List<WriteEntry> writes = new ArrayList<WriteEntry>();
        writes.add(entry);

        handleAsyncResume(response, metrics.write(writes), METRICS);
    }

    private static final Resume<MetricsQueryResponse, MetricsResponse> WRITE_METRICS = new Resume<MetricsQueryResponse, MetricsResponse>() {
        @Override
        public MetricsResponse resume(MetricsQueryResponse result)
                throws Exception {
            final MetricGroups groups = result.getMetricGroups();
            final Map<TimeSerie, List<DataPoint>> data = makeData(groups
                    .getGroups());
            return new MetricsResponse(result.getQueryRange(), data,
                    groups.getStatistics());
        }
    };

    @POST
    @Path("/metrics")
    public void metrics(@Suspended final AsyncResponse response,
            MetricsRequest query) throws MetricQueryException {
        log.info("POST /metrics: {}", query);

        final Callback<MetricsQueryResponse> callback = metrics
                .queryMetrics(query);

        handleAsyncResume(response, callback, WRITE_METRICS);
    }

    /**
     * Helper function to correctly wire up async response management.
     *
     * @param response
     *            The async response object.
     * @param callback
     *            Callback for the pending request.
     * @param resume
     *            The resume implementation.
     */
    private static <T, R> void handleAsyncResume(final AsyncResponse response,
            final Callback<T> callback, final Resume<T, R> resume) {
        callback.register(new Callback.Handle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                response.resume(Response
                        .status(Response.Status.GATEWAY_TIMEOUT)
                        .entity(new ErrorMessage("Request cancelled: " + reason))
                        .build());
            }

            @Override
            public void failed(Exception e) throws Exception {
                response.resume(Response
                        .status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(e).build());
            }

            @Override
            public void resolved(T result) throws Exception {
                response.resume(Response.status(Response.Status.OK)
                        .entity(resume.resume(result)).build());
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
    public void tags(@Suspended final AsyncResponse response, TagsRequest query)
            throws MetadataQueryException {
        if (!metadataBackend.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final TimeSerieQuery timeSeriesQuery = new TimeSerieQuery(
                query.getMatchKey(), query.getMatchTags(), query.getHasTags());

        metadataResult(
                response,
                metadataBackend.findTags(timeSeriesQuery, query.getInclude(),
                        query.getExclude()).transform(
                                new Callback.Transformer<FindTags, TagsResponse>() {
                                    @Override
                                    public TagsResponse transform(FindTags result)
                                            throws Exception {
                                        return new TagsResponse(result.getTags(),
                                                result.getSize());
                                    }
                                }));
    }

    @POST
    @Path("/keys")
    public void keys(@Suspended final AsyncResponse response,
            TimeSeriesRequest query) throws MetadataQueryException {
        if (!metadataBackend.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final TimeSerieQuery timeSeriesQuery = new TimeSerieQuery(
                query.getMatchKey(), query.getMatchTags(), query.getHasTags());

        metadataResult(response, metadataBackend.findKeys(timeSeriesQuery)
                .transform(new Callback.Transformer<FindKeys, KeysResponse>() {
                    @Override
                    public KeysResponse transform(FindKeys result)
                            throws Exception {
                        return new KeysResponse(result.getKeys(), result
                                .getSize());
                    }
                }));
    }

    @POST
    @Path("/timeseries")
    public void getTimeSeries(@Suspended final AsyncResponse response,
            TimeSeriesRequest query) throws MetadataQueryException {
        if (!metadataBackend.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final TimeSerieQuery timeSeriesQuery = new TimeSerieQuery(
                query.getMatchKey(), query.getMatchTags(), query.getHasTags());

        metadataResult(
                response,
                metadataBackend
                .findTimeSeries(timeSeriesQuery)
                .transform(
                        new Callback.Transformer<FindTimeSeries, TimeSeriesResponse>() {
                            @Override
                            public TimeSeriesResponse transform(
                                    FindTimeSeries result)
                                            throws Exception {
                                return new TimeSeriesResponse(
                                        new ArrayList<TimeSerie>(result
                                                .getTimeSeries()),
                                                result.getSize());
                            }
                        }));
    }

    /**
     * Encode/Decode functions, helpful when interacting with cassandra through
     * cqlsh.
     */

    @POST
    @Path("/decode/row-key")
    @Produces(MediaType.APPLICATION_JSON)
    public Response decodeRowKey(final DecodeRowKeyRequest request) {
        String data = request.getData();

        if (data.substring(0, 2).equals("0x"))
            data = data.substring(2, data.length());

        data = data.toUpperCase();

        final byte[] bytes = BaseEncoding.base16().decode(data);
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final MetricsRowKey rowKey = MetricsRowKeySerializer.get()
                .fromByteBuffer(buffer);
        return Response
                .status(Response.Status.OK)
                .entity(new DecodeRowKeyResponse(rowKey.getTimeSerie(), rowKey
                        .getBase())).build();
    }

    @POST
    @Path("/encode/row-key")
    @Produces(MediaType.APPLICATION_JSON)
    public Response encodeRowKey(final EncodeRowKeyRequest request) {
        final MetricsRowKey rowKey = new MetricsRowKey(request.getTimeSerie(),
                request.getBase());
        final ByteBuffer buffer = MetricsRowKeySerializer.get().toByteBuffer(
                rowKey);

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        final String data = "0x"
                + BaseEncoding.base16().encode(bytes).toLowerCase();
        return Response.status(Response.Status.OK)
                .entity(new EncodeRowKeyResponse(data)).build();
    }

    private <T> void metadataResult(final AsyncResponse response,
            Callback<T> callback) {
        callback.register(new Callback.Handle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                response.resume(Response
                        .status(Response.Status.GATEWAY_TIMEOUT)
                        .entity(new ErrorMessage("Request cancelled: " + reason))
                        .build());
            }

            @Override
            public void failed(Exception e) throws Exception {
                response.resume(Response
                        .status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(e).build());
            }

            @Override
            public void resolved(T result) throws Exception {
                response.resume(Response.status(Response.Status.OK)
                        .entity(result).build());
            }
        });
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
