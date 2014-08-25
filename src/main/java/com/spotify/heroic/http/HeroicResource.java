package com.spotify.heroic.http;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.http.model.IdResponse;
import com.spotify.heroic.http.model.MessageResponse;
import com.spotify.heroic.http.model.metadata.KeysResponse;
import com.spotify.heroic.http.model.metadata.TagsResponse;
import com.spotify.heroic.http.model.metadata.SeriesQuery;
import com.spotify.heroic.http.model.metadata.SeriesResponse;
import com.spotify.heroic.http.model.query.MetricsQuery;
import com.spotify.heroic.http.model.query.MetricsResponse;
import com.spotify.heroic.http.model.write.WriteMetrics;
import com.spotify.heroic.http.model.write.WriteMetricsResponse;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.MetadataQueryException;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.MetricQueryException;
import com.spotify.heroic.metrics.MetricStream;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.QueryMetricsResult;
import com.spotify.heroic.metrics.model.StreamMetricsResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
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
    public static class StoredMetricQueries {
        private final ConcurrentHashMap<String, StoredQuery> storedQueries = new ConcurrentHashMap<>();

        public void put(String id, StoredQuery query) {
            storedQueries.put(id, query);
        }

        public StoredQuery get(String id) {
            return storedQueries.get(id);
        }
    }

    @Data
    public static class StoredQuery {
        private final Filter filter;
        private final List<String> groupBy;
        private final DateRange range;
        private final AggregationGroup aggregation;
    }

    @Inject
    private MetricBackendManager metrics;

    @Inject
    private MetadataBackendManager metadata;

    @Inject
    private StoredMetricQueries storedQueries;

    @Data
    public static final class Message {
        private final String message;
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
            MetricsQuery query) throws MetricQueryException {
        final StoredQuery q = makeMetricsQuery(query);

        log.info("POST /metrics: {}", q);

        final Callback<QueryMetricsResult> callback = metrics
                .queryMetrics(q.getFilter(), q.getGroupBy(), q.getRange(),
                        q.getAggregation());

        response.setTimeout(300, TimeUnit.SECONDS);

        HttpAsyncUtils.handleAsyncResume(response, callback, WRITE_METRICS);
    }

    @POST
    @Path("/metrics-stream")
    @Produces(MediaType.APPLICATION_JSON)
    public Response makeMetricsStream(MetricsQuery query,
            @Context UriInfo info) throws MetricQueryException {
        final StoredQuery q = makeMetricsQuery(query);

        log.info("POST /metrics-stream: {}", q);

        final String id = Integer.toHexString(q.hashCode());
        storedQueries.put(id, q);

        final URI location = info.getBaseUriBuilder()
                .path("/metrics-stream/" + id).build();
        return Response.created(location).entity(new IdResponse<String>(id))
                .build();
    }

    private StoredQuery makeMetricsQuery(MetricsQuery query)
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

        final StoredQuery stored = new StoredQuery(filter, groupBy, range,
                aggregation);

        return stored;
    }

    @GET
    @Path("/metrics-stream/{id}")
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    public EventOutput getMetricsStream(@PathParam("id") String id)
            throws WebApplicationException, MetricQueryException {
        log.info("GET /metrics-stream/{}", id);

        final StoredQuery q = storedQueries.get(id);

        if (q == null)
            throw new NotFoundException("No such stored query: " + id);

        log.info("Query: {}", q);

        final EventOutput eventOutput = new EventOutput();

        final MetricStream handle = new MetricStream() {
            @Override
            public void stream(Callback<StreamMetricsResult> callback,
                    QueryMetricsResult result) throws Exception {
                if (eventOutput.isClosed()) {
                    callback.cancel(new CancelReason("client disconnected"));
                    return;
                }

                final MetricGroups groups = result.getMetricGroups();
                final Map<Series, List<DataPoint>> data = makeData(groups
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
                        new MessageResponse(reason.getMessage()));
            }

            @Override
            public void failed(Exception e) throws Exception {
                sendEvent(eventOutput, "error",
                        new MessageResponse(e.getMessage()));
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
            WriteMetrics write) {
        log.info("POST /write: {}", write);

        final WriteMetric entry = new WriteMetric(write.getSeries(),
                write.getData());

        final List<WriteMetric> writes = new ArrayList<WriteMetric>();
        writes.add(entry);

        HttpAsyncUtils.handleAsyncResume(response, metrics.write(writes),
                METRICS);
    }

    private static final HttpAsyncUtils.Resume<QueryMetricsResult, MetricsResponse> WRITE_METRICS = new HttpAsyncUtils.Resume<QueryMetricsResult, MetricsResponse>() {
        @Override
        public MetricsResponse resume(QueryMetricsResult result)
                throws Exception {
            final MetricGroups groups = result.getMetricGroups();
            final Map<Series, List<DataPoint>> data = makeData(groups
                    .getGroups());
            return new MetricsResponse(result.getQueryRange(), data,
                    groups.getStatistics());
        }
    };

    @POST
    @Path("/tags")
    public void tags(@Suspended final AsyncResponse response,
            SeriesQuery query) throws MetadataQueryException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new MessageResponse("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        log.info("/tags: {} {}", query, filter);

        final TimeSerieQuery seriesQuery = new TimeSerieQuery(filter);

        metadataResult(
                response,
                metadata.findTags(seriesQuery).transform(
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
            SeriesQuery query) throws MetadataQueryException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new MessageResponse("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        log.info("/keys: {} {}", query, filter);

        final TimeSerieQuery seriesQuery = new TimeSerieQuery(filter);

        metadataResult(
                response,
                metadata.findKeys(seriesQuery).transform(
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
            SeriesQuery query) throws MetadataQueryException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new MessageResponse("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        if (filter == null)
            throw new MetadataQueryException(
                    "Filter must not be empty when querying");

        log.info("/timeseries: {} {}", query, filter);

        final TimeSerieQuery seriesQuery = new TimeSerieQuery(filter);

        metadataResult(
                response,
                metadata.findTimeSeries(seriesQuery)
                .transform(
                        new Callback.Transformer<FindTimeSeries, SeriesResponse>() {
                            @Override
                            public SeriesResponse transform(
                                    FindTimeSeries result)
                                            throws Exception {
                                return new SeriesResponse(
                                        new ArrayList<Series>(result
                                                .getSeries()), result
                                                .getSize());
                            }
                        }));
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
    private Filter buildFilter(MetricsQuery query) {
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
                        .entity(new MessageResponse("Request cancelled: "
                                + reason)).build());
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

    private static Map<Series, List<DataPoint>> makeData(
            List<MetricGroup> groups) {
        final Map<Series, List<DataPoint>> data = new HashMap<Series, List<DataPoint>>();

        for (final MetricGroup group : groups) {
            data.put(group.getSeries(), group.getDatapoints());
        }

        return data;
    }
}
