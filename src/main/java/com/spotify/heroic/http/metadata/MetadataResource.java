package com.spotify.heroic.http.metadata;

import java.util.ArrayList;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.http.general.MessageResponse;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.MetadataQueryException;
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

    @POST
    @Path("/tags")
    public void tags(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataQueryException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new MessageResponse("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        log.info("/tags: {} {}", query, filter);

        final Callback.Transformer<FindTags, MetadataTagsResponse> transformer = new Callback.Transformer<FindTags, MetadataTagsResponse>() {
            @Override
            public MetadataTagsResponse transform(FindTags result)
                    throws Exception {
                return new MetadataTagsResponse(result.getTags(),
                        result.getSize());
            }
        };

        final Callback<MetadataTagsResponse> callback = metadata.findTags(
                filter).transform(transformer);

        metadataResult(response, callback);
    }

    @POST
    @Path("/keys")
    public void keys(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataQueryException {
        if (!metadata.isReady()) {
            response.resume(Response
                    .status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new MessageResponse("Cache is not ready")).build());
            return;
        }

        final Filter filter = query.makeFilter();

        log.info("/keys: {} {}", query, filter);

        final Callback.Transformer<FindKeys, MetadataKeysResponse> transformer = new Callback.Transformer<FindKeys, MetadataKeysResponse>() {
            @Override
            public MetadataKeysResponse transform(FindKeys result)
                    throws Exception {
                return new MetadataKeysResponse(result.getKeys(),
                        result.getSize());
            }
        };

        final Callback<MetadataKeysResponse> callback = metadata.findKeys(
                filter).transform(transformer);

        metadataResult(response, callback);
    }

    @POST
    @Path("/timeseries")
    public void getTimeSeries(@Suspended final AsyncResponse response,
            MetadataQueryBody query) throws MetadataQueryException {
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

        final Callback.Transformer<FindTimeSeries, MetadataSeriesResponse> transformer = new Callback.Transformer<FindTimeSeries, MetadataSeriesResponse>() {
            @Override
            public MetadataSeriesResponse transform(FindTimeSeries result)
                    throws Exception {
                return new MetadataSeriesResponse(new ArrayList<Series>(
                        result.getSeries()), result.getSize());
            }
        };

        final Callback<MetadataSeriesResponse> callback = metadata
                .findTimeSeries(filter).transform(transformer);

        metadataResult(response, callback);
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
}
