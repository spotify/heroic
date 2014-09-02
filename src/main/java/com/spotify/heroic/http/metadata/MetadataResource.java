package com.spotify.heroic.http.metadata;

import java.util.ArrayList;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.http.HttpAsyncUtils;
import com.spotify.heroic.http.general.ErrorMessage;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.MetadataQueryException;
import com.spotify.heroic.metadata.model.DeleteTimeSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.filter.Filter;

@Slf4j
@Path("/")
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
            MetadataQueryBody query) throws MetadataQueryException {
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
            MetadataQueryBody query) throws MetadataQueryException {
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

    private static final HttpAsyncUtils.Resume<FindTimeSeries, MetadataSeriesResponse> GET_TIMESERIES = new HttpAsyncUtils.Resume<FindTimeSeries, MetadataSeriesResponse>() {
        @Override
        public MetadataSeriesResponse resume(FindTimeSeries result)
                throws Exception {
            return new MetadataSeriesResponse(new ArrayList<Series>(
                    result.getSeries()), result.getSize());
        }
    };

    @POST
    @Path("/timeseries")
    public void getTimeSeries(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataQueryException {
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

        final Callback<FindTimeSeries> callback = metadata
                .findTimeSeries(filter);

        HttpAsyncUtils.handleAsyncResume(response, callback, GET_TIMESERIES);
    }

    private static final HttpAsyncUtils.Resume<DeleteTimeSeries, MetadataDeleteSeriesResponse> DELETE_TIMESERIES = new HttpAsyncUtils.Resume<DeleteTimeSeries, MetadataDeleteSeriesResponse>() {
        @Override
        public MetadataDeleteSeriesResponse resume(DeleteTimeSeries result)
                throws Exception {
            return new MetadataDeleteSeriesResponse(result.getSuccessful(),
                    result.getFailed());
        }
    };

    @DELETE
    @Path("/timeseries")
    public void deleteTimeSeries(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataQueryException {
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

        final Callback<DeleteTimeSeries> callback = metadata
                .deleteTimeSeries(filter);

        HttpAsyncUtils.handleAsyncResume(response, callback, DELETE_TIMESERIES);
    }
}
