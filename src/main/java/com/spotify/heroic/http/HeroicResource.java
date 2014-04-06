package com.spotify.heroic.http;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.TimeSeriesCache;
import com.spotify.heroic.query.KeysResponse;
import com.spotify.heroic.query.MetricsQuery;
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
    public void metrics(@Suspended AsyncResponse response, MetricsQuery query) {
        log.info("Query: " + query);
        backendManager.queryMetrics(query, response);
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
