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

import static java.util.Optional.ofNullable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.metric.WriteResult;

import eu.toolchain.async.AsyncFuture;

@Path("/metadata")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetadataResource {
    @Inject
    private FilterFactory filters;

    @Inject
    private JavaxRestFramework httpAsync;

    @Inject
    private ClusterManager cluster;

    @Inject
    private MetadataResourceCache cache;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @POST
    @Path("/tags")
    public void tags(@Suspended final AsyncResponse response, final MetadataQueryBody body)
            throws ExecutionException {
        final MetadataQueryBody request =
                ofNullable(body).orElseGet(MetadataQueryBody::createDefault);
        final RangeFilter filter =
                toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cache.findTags(null, filter));
    }

    @POST
    @Path("/keys")
    public void keys(@Suspended final AsyncResponse response, final MetadataQueryBody body)
            throws ExecutionException {
        final MetadataQueryBody request =
                ofNullable(body).orElseGet(MetadataQueryBody::createDefault);
        final RangeFilter filter =
                toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cache.findKeys(null, filter));
    }

    @PUT
    @Path("/series")
    public void addSeries(@Suspended final AsyncResponse response, final Series series) {
        if (series == null) {
            throw new IllegalStateException("No series specified");
        }

        final DateRange range = DateRange.now();
        final AsyncFuture<WriteResult> callback =
                cluster.useDefaultGroup().writeSeries(range, series);
        httpAsync.bind(response, callback);
    }

    @POST
    @Path("/series")
    public void getTimeSeries(@Suspended final AsyncResponse response, final MetadataQueryBody body)
            throws JsonParseException, JsonMappingException, IOException {
        final MetadataQueryBody request =
                ofNullable(body).orElseGet(MetadataQueryBody::createDefault);
        final RangeFilter filter =
                toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cluster.useDefaultGroup().findSeries(filter));
    }

    @DELETE
    @Path("/series")
    public void deleteTimeSeries(@Suspended final AsyncResponse response,
            final MetadataQueryBody body) {
        final MetadataQueryBody request =
                ofNullable(body).orElseGet(MetadataQueryBody::createDefault);
        final RangeFilter filter = toRangeFilter(request::getFilter, request::getRange);
        httpAsync.bind(response, cluster.useDefaultGroup().deleteSeries(filter));
    }

    @POST
    @Path("series-count")
    public void seriesCount(@Suspended final AsyncResponse response, final MetadataCount body) {
        final MetadataCount request = ofNullable(body).orElseGet(MetadataCount::createDefault);
        final RangeFilter filter = toRangeFilter(request::getFilter, request::getRange);
        httpAsync.bind(response, cluster.useDefaultGroup().countSeries(filter));
    }

    @POST
    @Path("tagkey-count")
    public void tagkeyCount(@Suspended final AsyncResponse response,
            final MetadataTagKeySuggest body) {
        final MetadataTagKeySuggest request =
                ofNullable(body).orElseGet(MetadataTagKeySuggest::createDefault);
        final RangeFilter filter =
                toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cluster.useDefaultGroup().tagKeyCount(filter));
    }

    @POST
    @Path("key-suggest")
    public void keySuggest(@Suspended final AsyncResponse response, final MetadataKeySuggest body) {
        final MetadataKeySuggest request =
                ofNullable(body).orElseGet(MetadataKeySuggest::createDefault);
        final RangeFilter filter =
                toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response,
                cluster.useDefaultGroup().keySuggest(filter, request.getMatch(), request.getKey()));
    }

    @POST
    @Path("tag-suggest")
    public void tagSuggest(@Suspended final AsyncResponse response, final MetadataTagSuggest body) {
        final MetadataTagSuggest request =
                ofNullable(body).orElseGet(MetadataTagSuggest::createDefault);
        final RangeFilter filter =
                toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cluster.useDefaultGroup().tagSuggest(filter, request.getMatch(),
                request.getKey(), request.getValue()));
    }

    @POST
    @Path("tag-value-suggest")
    public void tagValueSuggest(@Suspended final AsyncResponse response,
            final MetadataTagValueSuggest body) {
        final MetadataTagValueSuggest request =
                ofNullable(body).orElseGet(MetadataTagValueSuggest::createDefault);
        final RangeFilter filter =
                toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response,
                cluster.useDefaultGroup().tagValueSuggest(filter, request.getKey()));
    }

    @POST
    @Path("tag-values-suggest")
    public void tagValuesSuggest(@Suspended final AsyncResponse response,
            final MetadataTagValuesSuggest body) {
        final MetadataTagValuesSuggest request =
                ofNullable(body).orElseGet(MetadataTagValuesSuggest::createDefault);
        final RangeFilter filter =
                toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cluster.useDefaultGroup().tagValuesSuggest(filter,
                request.getExclude(), request.getGroupLimit()));
    }

    private RangeFilter toRangeFilter(final Supplier<Optional<Filter>> optionalFilter,
            final Supplier<Optional<QueryDateRange>> optionalRange) {
        return toRangeFilter(optionalFilter, optionalRange, () -> Integer.MAX_VALUE);
    }

    private RangeFilter toRangeFilter(final Supplier<Optional<Filter>> optionalFilter,
            final Supplier<Optional<QueryDateRange>> optionalRange, final Supplier<Integer> limit) {
        final long now = System.currentTimeMillis();
        final Filter filter = optionalFilter.get().orElseGet(filters::t);
        final Optional<DateRange> range = optionalRange.get().map(r -> r.buildDateRange(now));
        return RangeFilter.filterFor(filter, range, now, limit.get());
    }
}
