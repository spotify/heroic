/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.http.metadata;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.metadata.model.CountSeries;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.suggest.model.KeySuggest;
import com.spotify.heroic.suggest.model.TagKeyCount;
import com.spotify.heroic.suggest.model.TagSuggest;
import com.spotify.heroic.suggest.model.TagValueSuggest;
import com.spotify.heroic.suggest.model.TagValuesSuggest;
import com.spotify.heroic.utils.HttpAsyncUtils;

import eu.toolchain.async.AsyncFuture;

@Slf4j
@Path("/metadata")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetadataResource {
    @Inject
    private FilterFactory filters;

    @Inject
    private HttpAsyncUtils httpAsync;

    @Inject
    private ClusterManager cluster;

    @Inject
    private MetadataResourceCache cache;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @POST
    @Path("/tags")
    public void tags(@Suspended final AsyncResponse response, MetadataQueryBody query) throws ExecutionException {
        if (query == null)
            query = MetadataQueryBody.create();

        final Filter filter = query.makeFilter(filters);

        log.info("/tags: {} {}", query, filter);

        final AsyncFuture<FindTags> callback = cache.findTags(null, RangeFilter.filterFor(filter, query.getRange()));

        httpAsync.handleAsyncResume(response, callback);
    }

    @POST
    @Path("/keys")
    public void keys(@Suspended final AsyncResponse response, MetadataQueryBody query) throws ExecutionException {
        if (query == null)
            query = MetadataQueryBody.create();

        final Filter filter = query.makeFilter(filters);

        log.info("/keys: {} {}", query, filter);

        final AsyncFuture<FindKeys> callback = cache.findKeys(null, RangeFilter.filterFor(filter, query.getRange()));

        httpAsync.handleAsyncResume(response, callback);
    }

    private static final HttpAsyncUtils.Resume<WriteResult, MetadataAddSeriesResponse> WRITE = new HttpAsyncUtils.Resume<WriteResult, MetadataAddSeriesResponse>() {
        @Override
        public MetadataAddSeriesResponse resume(WriteResult value) throws Exception {
            return new MetadataAddSeriesResponse(value.getTimes());
        }
    };

    @PUT
    @Path("/series")
    public void addSeries(@Suspended final AsyncResponse response, Series series) {
        final DateRange range = DateRange.now();
        final AsyncFuture<WriteResult> callback = cluster.useDefaultGroup().writeSeries(range, series);
        httpAsync.handleAsyncResume(response, callback, WRITE);
    }

    @POST
    @Path("/series")
    public void getTimeSeries(@Suspended final AsyncResponse response, MetadataQueryBody query)
            throws JsonParseException, JsonMappingException, IOException {
        if (query == null)
            query = MetadataQueryBody.create();

        final Filter filter = query.makeFilter(filters);

        if (filter == null) {
            throw new IllegalArgumentException("Filter must not be empty when querying");
        }

        log.info("/timeseries: {} {}", query, filter);

        final AsyncFuture<FindSeries> callback = cluster.useDefaultGroup().findSeries(
                RangeFilter.filterFor(filter, query.getRange(), query.getLimit()));

        httpAsync.handleAsyncResume(response, callback);
    }

    @DELETE
    @Path("/series")
    public void deleteTimeSeries(@Suspended final AsyncResponse response, MetadataQueryBody query) {
        if (query == null)
            query = MetadataQueryBody.create();

        final Filter filter = query.makeFilter(filters);

        if (filter == null)
            throw new IllegalArgumentException("Filter must not be empty when querying");

        log.info("/timeseries: {} {}", query, filter);

        final AsyncFuture<DeleteSeries> callback = cluster.useDefaultGroup().deleteSeries(
                RangeFilter.filterFor(filter, query.getRange()));

        httpAsync.handleAsyncResume(response, callback);
    }

    @POST
    @Path("series-count")
    public void seriesCount(@Suspended final AsyncResponse response, MetadataCount request) {
        if (request == null)
            request = MetadataCount.createDefault();

        final AsyncFuture<CountSeries> callback = cluster.useDefaultGroup().countSeries(
                RangeFilter.filterFor(request.getFilter(), request.getRange()));

        httpAsync.handleAsyncResume(response, callback);
    }

    @POST
    @Path("tagkey-count")
    public void tagkeyCount(@Suspended final AsyncResponse response, MetadataTagKeySuggest request) {
        if (request == null)
            request = MetadataTagKeySuggest.createDefault();

        final AsyncFuture<TagKeyCount> callback = cluster.useDefaultGroup().tagKeyCount(
                RangeFilter.filterFor(request.getFilter(), request.getRange(), request.getLimit()));

        httpAsync.handleAsyncResume(response, callback);
    }

    @POST
    @Path("key-suggest")
    public void keySuggest(@Suspended final AsyncResponse response, MetadataKeySuggest request) {
        if (request == null)
            request = MetadataKeySuggest.createDefault();

        final AsyncFuture<KeySuggest> callback = cluster.useDefaultGroup().keySuggest(
                RangeFilter.filterFor(request.getFilter(), request.getRange(), request.getLimit()), request.getMatch(),
                request.getKey());

        httpAsync.handleAsyncResume(response, callback);
    }

    /* @POST
     * 
     * @Path("tagkey-suggest") public void tagKeySuggest(@Suspended final AsyncResponse response, MetadataTagKeySuggest
     * request) { if (request == null) request = MetadataTagSuggest.createDefault();
     * 
     * final AsyncFuture<TagKeySuggest> callback = suggest.tagKeySuggest(null,
     * RangeFilter.filterFor(request.getFilter(), request.getRange(), request.getLimit()), request.getMatch(),
     * request.getValue());
     * 
     * httpAsync.handleAsyncResume(response, callback); } */

    @POST
    @Path("tag-suggest")
    public void tagSuggest(@Suspended final AsyncResponse response, MetadataTagSuggest request) {
        if (request == null)
            request = MetadataTagSuggest.createDefault();

        final AsyncFuture<TagSuggest> callback = cluster.useDefaultGroup().tagSuggest(
                RangeFilter.filterFor(request.getFilter(), request.getRange(), request.getLimit()), request.getMatch(),
                request.getKey(), request.getValue());

        httpAsync.handleAsyncResume(response, callback);
    }

    @POST
    @Path("tag-value-suggest")
    public void tagValueSuggest(@Suspended final AsyncResponse response, MetadataTagValueSuggest request) {
        if (request == null)
            request = MetadataTagValueSuggest.createDefault();

        final AsyncFuture<TagValueSuggest> callback = cluster.useDefaultGroup().tagValueSuggest(
                RangeFilter.filterFor(request.getFilter(), request.getRange(), request.getLimit()), request.getKey());

        httpAsync.handleAsyncResume(response, callback);
    }

    @POST
    @Path("tag-values-suggest")
    public void tagValuesSuggest(@Suspended final AsyncResponse response, MetadataTagValuesSuggest request) {
        if (request == null)
            request = MetadataTagValuesSuggest.createDefault();

        final AsyncFuture<TagValuesSuggest> callback = cluster.useDefaultGroup().tagValuesSuggest(
                RangeFilter.filterFor(request.getFilter(), request.getRange(), request.getLimit()),
                request.getExclude(), request.getGroupLimit());

        httpAsync.handleAsyncResume(response, callback);
    }
}
