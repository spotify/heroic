package com.spotify.heroic.http;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseFeature;

import com.google.common.io.BaseEncoding;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.http.model.DecodeRowKeyRequest;
import com.spotify.heroic.http.model.DecodeRowKeyResponse;
import com.spotify.heroic.http.model.EncodeRowKeyRequest;
import com.spotify.heroic.http.model.EncodeRowKeyResponse;
import com.spotify.heroic.http.model.KeysResponse;
import com.spotify.heroic.http.model.MetricsQuery;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.http.model.MetricsRequest;
import com.spotify.heroic.http.model.MetricsResponse;
import com.spotify.heroic.http.model.MetricsStreamResponse;
import com.spotify.heroic.http.model.TagsResponse;
import com.spotify.heroic.http.model.TimeSeriesRequest;
import com.spotify.heroic.http.model.TimeSeriesResponse;
import com.spotify.heroic.http.model.WriteMetricsRequest;
import com.spotify.heroic.http.model.WriteMetricsResponse;
import com.spotify.heroic.http.model.status.BackendStatusResponse;
import com.spotify.heroic.http.model.status.ClusterStatusResponse;
import com.spotify.heroic.http.model.status.ConsumerStatusResponse;
import com.spotify.heroic.http.model.status.MetadataBackendStatusResponse;
import com.spotify.heroic.http.model.status.StatusResponse;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.MetadataQueryException;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.MetricQueryException;
import com.spotify.heroic.metrics.MetricStream;
import com.spotify.heroic.metrics.heroic.MetricsRowKey;
import com.spotify.heroic.metrics.heroic.MetricsRowKeySerializer;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.StreamMetricsResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteMetric;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.model.filter.AndFilter;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.model.filter.MatchKeyFilter;
import com.spotify.heroic.model.filter.MatchTagFilter;

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
    private StoredMetricQueries storedQueries;

    @Inject
    private Set<Consumer> consumers;

    @Inject
    private Set<Backend> backends;

    @Inject
    private Set<MetadataBackend> metadataBackends;

    @Inject
    private ClusterManager cluster;

    @Data
    public static final class Message {
        private final String message;
    }

    @GET
    @Path("/status")
    public Response status() {
        final ConsumerStatusResponse consumers = buildConsumerStatus();
        final BackendStatusResponse backends = buildBackendStatus();
        final MetadataBackendStatusResponse metadataBackends = buildMetadataBackendStatus();

        final ClusterStatusResponse cluster = buildClusterStatus();

        final boolean allOk = consumers.isOk() && backends.isOk()
                && metadataBackends.isOk() && cluster.isOk();

        final StatusResponse response = new StatusResponse(allOk, consumers,
                backends, metadataBackends, cluster);

        return Response.status(Response.Status.OK).entity(response).build();
    }

    private ClusterStatusResponse buildClusterStatus() {
        if (cluster == ClusterManager.NULL)
            return new ClusterStatusResponse(true, 0, 0);

        final ClusterManager.Statistics s = cluster.getStatistics();

        if (s == null)
            return new ClusterStatusResponse(true, 0, 0);

        return new ClusterStatusResponse(s.getOfflineNodes() == 0,
                s.getOnlineNodes(), s.getOfflineNodes());
    }

    private BackendStatusResponse buildBackendStatus() {
        final int available = backends.size();

        int ready = 0;

        for (final Backend backend : backends) {
            if (backend.isReady())
                ready += 1;
        }

        return new BackendStatusResponse(available == ready, available, ready);
    }

    private ConsumerStatusResponse buildConsumerStatus() {
        final int available = consumers.size();

        int ready = 0;

        for (final Consumer consumer : consumers) {
            if (consumer.isReady())
                ready += 1;
        }

        return new ConsumerStatusResponse(available == ready, available, ready);
    }

    private MetadataBackendStatusResponse buildMetadataBackendStatus() {
        final int available = metadataBackends.size();

        int ready = 0;

        for (final MetadataBackend metadata : metadataBackends) {
            if (metadata.isReady())
                ready += 1;
        }

        return new MetadataBackendStatusResponse(available == ready, available,
                ready);
    }

    @POST
    @Path("/shutdown")
    public Response shutdown() {
        return Response.status(Response.Status.OK)
                .entity(new Message("shutting down")).build();
    }

    @POST
    @Path("/metrics")
    public void metrics(@Suspended final AsyncResponse response,
            MetricsRequest query) throws MetricQueryException {
        final MetricsQuery q = makeMetricsQuery(query);

        log.info("POST /metrics: {}", q);

        final Callback<MetricsQueryResponse> callback = metrics
                .queryMetrics(q.getFilter(), q.getGroupBy(), q.getRange(),
                        q.getAggregation());

        response.setTimeout(300, TimeUnit.SECONDS);

        HttpAsyncUtils.handleAsyncResume(response, callback, WRITE_METRICS);
    }

    @POST
    @Path("/metrics-stream")
    @Produces(MediaType.APPLICATION_JSON)
    public Response makeMetricsStream(MetricsRequest query,
            @Context UriInfo info) throws MetricQueryException {
        final MetricsQuery q = makeMetricsQuery(query);

        log.info("POST /metrics-stream: {}", q);

        final String id = Integer.toHexString(q.hashCode());
        storedQueries.put(id, q);

        final URI location = info.getBaseUriBuilder()
                .path("/metrics-stream/" + id).build();
        final MetricsStreamResponse entity = new MetricsStreamResponse(id);
        return Response.created(location).entity(entity).build();
    }

    private MetricsQuery makeMetricsQuery(MetricsRequest query)
            throws MetricQueryException {
        if (query == null)
            throw new MetricQueryException("Query must be defined");

        if (query.getRange() == null)
            throw new MetricQueryException("Range must be specified");

        final DateRange range = query.getRange().buildDateRange();

        if (!(range.start() < range.end()))
            throw new MetricQueryException(
                    "Range start must come before its end");

        final AggregationGroup aggregation;

        {
            final List<Aggregation> aggregators = query.makeAggregators();

            if (aggregators == null || aggregators.isEmpty()) {
                aggregation = null;
            } else {
                aggregation = new AggregationGroup(aggregators, aggregators
                        .get(0).getSampling());
            }
        }

        final List<String> groupBy = query.getGroupBy();
        final Filter filter = buildFilter(query);

        if (filter == null)
            throw new MetricQueryException(
                    "Filter must not be empty when querying");

        final MetricsQuery stored = new MetricsQuery(filter, groupBy, range,
                aggregation);

        return stored;
    }

    @GET
    @Path("/metrics-stream/{id}")
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    public EventOutput getMetricsStream(@PathParam("id") String id)
            throws WebApplicationException, MetricQueryException {
        log.info("GET /metrics-stream/{}", id);

        final MetricsQuery q = storedQueries.get(id);

        if (q == null)
            throw new NotFoundException("No such stored query: " + id);

        log.info("Query: {}", q);

        final EventOutput eventOutput = new EventOutput();

        final MetricStream handle = new MetricStream() {
            @Override
            public void stream(Callback<StreamMetricsResult> callback,
                    MetricsQueryResponse result) throws Exception {
                if (eventOutput.isClosed()) {
                    callback.cancel(new CancelReason("client disconnected"));
                    return;
                }

                final MetricGroups groups = result.getMetricGroups();
                final Map<TimeSerie, List<DataPoint>> data = makeData(groups
                        .getGroups());
                final MetricsResponse entity = new MetricsResponse(
                        result.getQueryRange(), data, groups.getStatistics());
                final OutboundEvent.Builder builder = new OutboundEvent.Builder();

                builder.mediaType(MediaType.APPLICATION_JSON_TYPE);
                builder.name("metrics");
                builder.data(MetricsResponse.class, entity);
                eventOutput.write(builder.build());
            }
        };

        final Callback<StreamMetricsResult> callback = metrics.streamMetrics(
                q.getFilter(), q.getGroupBy(), q.getRange(),
                q.getAggregation(), handle);

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

    private static final HttpAsyncUtils.Resume<WriteResponse, WriteMetricsResponse> METRICS = new HttpAsyncUtils.Resume<WriteResponse, WriteMetricsResponse>() {
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

        final WriteMetric entry = new WriteMetric(write.getTimeSerie(),
                write.getData());

        final List<WriteMetric> writes = new ArrayList<WriteMetric>();
        writes.add(entry);

        HttpAsyncUtils.handleAsyncResume(response, metrics.write(writes),
                METRICS);
    }

    private static final HttpAsyncUtils.Resume<MetricsQueryResponse, MetricsResponse> WRITE_METRICS = new HttpAsyncUtils.Resume<MetricsQueryResponse, MetricsResponse>() {
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
    @Path("/tags")
    public void tags(@Suspended final AsyncResponse response,
            TimeSeriesRequest query) throws MetadataQueryException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        log.info("/tags: {} {}", query, filter);

        final TimeSerieQuery timeSeriesQuery = new TimeSerieQuery(filter);

        metadataResult(
                response,
                metadata.findTags(timeSeriesQuery).transform(
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
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        log.info("/keys: {} {}", query, filter);

        final TimeSerieQuery timeSeriesQuery = new TimeSerieQuery(filter);

        metadataResult(
                response,
                metadata.findKeys(timeSeriesQuery).transform(
                        new Callback.Transformer<FindKeys, KeysResponse>() {
                            @Override
                            public KeysResponse transform(FindKeys result)
                                    throws Exception {
                                return new KeysResponse(result.getKeys(),
                                        result.getSize());
                            }
                        }));
    }

    @POST
    @Path("/timeseries")
    public void getTimeSeries(@Suspended final AsyncResponse response,
            TimeSeriesRequest query) throws MetadataQueryException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        if (filter == null)
            throw new MetadataQueryException(
                    "Filter must not be empty when querying");

        log.info("/timeseries: {} {}", query, filter);

        final TimeSerieQuery timeSeriesQuery = new TimeSerieQuery(filter);

        metadataResult(
                response,
                metadata.findTimeSeries(timeSeriesQuery)
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

        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        final String data = "0x"
                + BaseEncoding.base16().encode(bytes).toLowerCase();
        return Response.status(Response.Status.OK)
                .entity(new EncodeRowKeyResponse(data)).build();
    }

    /**
     * Convert a MetricsRequest into a filter.
     *
     * This is meant to stay backwards compatible, since every filtering in
     * MetricsRequest can be expressed as filter objects.
     *
     * @param query
     * @return
     */
    private Filter buildFilter(MetricsRequest query) {
        final List<Filter> statements = new ArrayList<>();

        if (query.getTags() != null && !query.getTags().isEmpty()) {
            for (final Map.Entry<String, String> entry : query.getTags()
                    .entrySet()) {
                statements.add(new MatchTagFilter(entry.getKey(), entry
                        .getValue()));
            }
        }

        if (query.getKey() != null)
            statements.add(new MatchKeyFilter(query.getKey()));

        if (query.getFilter() != null)
            statements.add(query.getFilter());

        if (statements.size() == 0)
            return null;

        if (statements.size() == 1)
            return statements.get(0);

        return new AndFilter(statements).optimize();
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
