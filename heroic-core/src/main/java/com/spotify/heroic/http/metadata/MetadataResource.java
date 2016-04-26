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

import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Path("metadata")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetadataResource {
    private final FilterFactory filters;
    private final JavaxRestFramework httpAsync;
    private final ClusterManager cluster;
    private final MetadataResourceCache cache;

    @Inject
    public MetadataResource(
        FilterFactory filters, JavaxRestFramework httpAsync, ClusterManager cluster,
        MetadataResourceCache cache
    ) {
        this.filters = filters;
        this.httpAsync = httpAsync;
        this.cluster = cluster;
        this.cache = cache;
    }

    @POST
    @Path("tags")
    public void tags(@Suspended final AsyncResponse response, final MetadataQueryBody request)
        throws ExecutionException {
        final RangeFilter filter =
            toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cache.findTags(null, filter));
    }

    @POST
    @Path("keys")
    public void keys(@Suspended final AsyncResponse response, final MetadataQueryBody request)
        throws ExecutionException {
        final RangeFilter filter =
            toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cache.findKeys(null, filter));
    }

    @PUT
    @Path("series")
    public void addSeries(@Suspended final AsyncResponse response, final Series series) {
        final DateRange range = DateRange.now();
        httpAsync.bind(response, cluster.useDefaultGroup().writeSeries(range, series));
    }

    @POST
    @Path("series")
    public void getTimeSeries(
        @Suspended final AsyncResponse response, final MetadataQueryBody request
    ) {
        final RangeFilter filter =
            toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cluster.useDefaultGroup().findSeries(filter));
    }

    @DELETE
    @Path("series")
    public void deleteTimeSeries(
        @Suspended final AsyncResponse response, final MetadataQueryBody request
    ) {
        final RangeFilter filter = toRangeFilter(request::getFilter, request::getRange);
        httpAsync.bind(response, cluster.useDefaultGroup().deleteSeries(filter));
    }

    @POST
    @Path("series-count")
    public void seriesCount(@Suspended final AsyncResponse response, final MetadataCount request) {
        final RangeFilter filter = toRangeFilter(request::getFilter, request::getRange);
        httpAsync.bind(response, cluster.useDefaultGroup().countSeries(filter));
    }

    @POST
    @Path("tagkey-count")
    public void tagkeyCount(
        @Suspended final AsyncResponse response, final MetadataTagKeySuggest request
    ) {
        final RangeFilter filter =
            toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cluster.useDefaultGroup().tagKeyCount(filter));
    }

    @POST
    @Path("key-suggest")
    public void keySuggest(
        @Suspended final AsyncResponse response, final MetadataKeySuggest request
    ) {
        final RangeFilter filter =
            toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response,
            cluster.useDefaultGroup().keySuggest(filter, request.getMatch(), request.getKey()));
    }

    @POST
    @Path("tag-suggest")
    public void tagSuggest(
        @Suspended final AsyncResponse response, final MetadataTagSuggest request
    ) {
        final RangeFilter filter =
            toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cluster
            .useDefaultGroup()
            .tagSuggest(filter, request.getMatch(), request.getKey(), request.getValue()));
    }

    @POST
    @Path("tag-value-suggest")
    public void tagValueSuggest(
        @Suspended final AsyncResponse response, final MetadataTagValueSuggest request
    ) {
        final RangeFilter filter =
            toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response,
            cluster.useDefaultGroup().tagValueSuggest(filter, request.getKey()));
    }

    @POST
    @Path("tag-values-suggest")
    public void tagValuesSuggest(
        @Suspended final AsyncResponse response, final MetadataTagValuesSuggest request
    ) {
        final RangeFilter filter =
            toRangeFilter(request::getFilter, request::getRange, request::getLimit);
        httpAsync.bind(response, cluster
            .useDefaultGroup()
            .tagValuesSuggest(filter, request.getExclude(), request.getGroupLimit()));
    }

    private RangeFilter toRangeFilter(
        final Supplier<Optional<Filter>> optionalFilter,
        final Supplier<Optional<QueryDateRange>> optionalRange
    ) {
        return toRangeFilter(optionalFilter, optionalRange, () -> Integer.MAX_VALUE);
    }

    private RangeFilter toRangeFilter(
        final Supplier<Optional<Filter>> optionalFilter,
        final Supplier<Optional<QueryDateRange>> optionalRange, final Supplier<Integer> limit
    ) {
        final long now = System.currentTimeMillis();
        final Filter filter = optionalFilter.get().orElseGet(filters::t);
        final DateRange range =
            optionalRange.get().map(r -> r.buildDateRange(now)).orElseGet(() -> {
                return new DateRange(now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS), now);
            });

        return new RangeFilter(filter, range, limit.get());
    }
}
