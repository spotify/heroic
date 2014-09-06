package com.spotify.heroic.http.metadata;

import java.io.IOException;
import java.util.ArrayList;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.http.HttpAsyncUtils;
import com.spotify.heroic.http.general.ErrorMessage;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.MetadataOperationException;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.model.Series;

@Slf4j
@Path("/metadata")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetadataResource {
    @Inject
    private MetadataBackendManager metadata;

    private static final HttpAsyncUtils.Resume<FindTags, MetadataTagsResponse> TAGS = new HttpAsyncUtils.Resume<FindTags, MetadataTagsResponse>() {
        @Override
        public MetadataTagsResponse resume(FindTags result) throws Exception {
            return new MetadataTagsResponse(result.getTags(), result.getSize());
        }
    };

    @POST
    @Path("/tags")
    public void tags(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataOperationException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        log.info("/tags: {} {}", query, filter);

        final Callback<FindTags> callback = metadata.findTags(filter);

        HttpAsyncUtils.handleAsyncResume(response, callback, TAGS);
    }

    private static final HttpAsyncUtils.Resume<FindKeys, MetadataKeysResponse> KEYS = new HttpAsyncUtils.Resume<FindKeys, MetadataKeysResponse>() {
        @Override
        public MetadataKeysResponse resume(FindKeys result) throws Exception {
            return new MetadataKeysResponse(result.getKeys(), result.getSize());
        }
    };

    @POST
    @Path("/keys")
    public void keys(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataOperationException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        log.info("/keys: {} {}", query, filter);

        final Callback<FindKeys> callback = metadata.findKeys(filter);

        HttpAsyncUtils.handleAsyncResume(response, callback, KEYS);
    }

    @POST
    @Path("/series")
    public Response addSeries(Series series) throws MetadataOperationException {
        if (!metadata.isReady()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build();
        }

        final String id = metadata.write(series);
        return Response.status(Response.Status.OK)
                .entity(new MetadataAddSeriesResponse(id)).build();
    }

    private static final HttpAsyncUtils.Resume<FindSeries, MetadataSeriesResponse> GET_SERIES = new HttpAsyncUtils.Resume<FindSeries, MetadataSeriesResponse>() {
        @Override
        public MetadataSeriesResponse resume(FindSeries result)
                throws Exception {
            return new MetadataSeriesResponse(new ArrayList<Series>(
                    result.getSeries()), result.getSize());
        }
    };

    @GET
    @Path("/series")
    public void getTimeSeries(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataOperationException,
            JsonParseException, JsonMappingException, IOException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        if (filter == null)
            throw new MetadataOperationException(
                    "Filter must not be empty when querying");

        log.info("/timeseries: {} {}", query, filter);

        final Callback<FindSeries> callback = metadata.findSeries(filter);

        HttpAsyncUtils.handleAsyncResume(response, callback, GET_SERIES);
    }

    private static final HttpAsyncUtils.Resume<DeleteSeries, MetadataDeleteSeriesResponse> DELETE_SERIES = new HttpAsyncUtils.Resume<DeleteSeries, MetadataDeleteSeriesResponse>() {
        @Override
        public MetadataDeleteSeriesResponse resume(DeleteSeries result)
                throws Exception {
            return new MetadataDeleteSeriesResponse(result.getSuccessful(),
                    result.getFailed());
        }
    };

    @DELETE
    @Path("/series")
    public void deleteTimeSeries(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataOperationException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ErrorMessage("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        if (filter == null)
            throw new MetadataOperationException(
                    "Filter must not be empty when querying");

        log.info("/timeseries: {} {}", query, filter);

        final Callback<DeleteSeries> callback = metadata.deleteSeries(filter);

        HttpAsyncUtils.handleAsyncResume(response, callback, DELETE_SERIES);
    }
}
