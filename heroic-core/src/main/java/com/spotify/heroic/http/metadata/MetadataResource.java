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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import com.spotify.heroic.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
import lombok.Data;

@Path("metadata")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetadataResource {
    private final Clock clock;
    private final JavaxRestFramework httpAsync;
    private final QueryManager query;
    private final MetadataResourceCache cache;

    @Inject
    public MetadataResource(
        Clock clock, JavaxRestFramework httpAsync, QueryManager query, MetadataResourceCache cache
    ) {
        this.clock = clock;
        this.httpAsync = httpAsync;
        this.query = query;
        this.cache = cache;
    }

    @POST
    @Path("tags")
    public void tags(@Suspended final AsyncResponse response, final MetadataQueryBody request)
        throws ExecutionException {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange,
            () -> OptionalLimit.of(request.getLimit().orElse(MetadataQueryBody.DEFAULT_LIMIT)));

        httpAsync.bind(response, cache.findTags(Optional.empty(),
            new FindTags.Request(c.getFilter(), c.getRange(), c.getLimit())));
    }

    @POST
    @Path("keys")
    public void keys(@Suspended final AsyncResponse response, final MetadataQueryBody request)
        throws ExecutionException {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange,
            () -> OptionalLimit.of(request.getLimit().orElse(MetadataQueryBody.DEFAULT_LIMIT)));

        httpAsync.bind(response, cache.findKeys(Optional.empty(),
            new FindKeys.Request(c.getFilter(), c.getRange(), c.getLimit())));
    }

    @PUT
    @Path("series")
    public void addSeries(@Suspended final AsyncResponse response, final Series series) {
        final DateRange range = DateRange.now(clock);
        httpAsync.bind(response,
            query.useDefaultGroup().writeSeries(new WriteMetadata.Request(series, range)));
    }

    @POST
    @Path("series")
    public void getTimeSeries(
        @Suspended final AsyncResponse response, final MetadataQueryBody request
    ) {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange,
            () -> OptionalLimit.of(request.getLimit().orElse(MetadataQueryBody.DEFAULT_LIMIT)));

        httpAsync.bind(response, query
            .useDefaultGroup()
            .findSeries(new FindSeries.Request(c.getFilter(), c.getRange(), c.getLimit())));
    }

    @DELETE
    @Path("series")
    public void deleteTimeSeries(
        @Suspended final AsyncResponse response, final MetadataQueryBody request
    ) {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange);
        httpAsync.bind(response, query
            .useDefaultGroup()
            .deleteSeries(new DeleteSeries.Request(c.getFilter(), c.getRange(), c.getLimit())));
    }

    @POST
    @Path("series-count")
    public void seriesCount(@Suspended final AsyncResponse response, final MetadataCount request) {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange);
        httpAsync.bind(response, query
            .useDefaultGroup()
            .countSeries(new CountSeries.Request(c.getFilter(), c.getRange(), c.getLimit())));
    }

    @POST
    @Path("tagkey-count")
    public void tagkeyCount(
        @Suspended final AsyncResponse response, final MetadataTagKeySuggest request
    ) {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange,
            () -> OptionalLimit.of(request.getLimit().orElse(MetadataTagKeySuggest.DEFAULT_LIMIT)));
        httpAsync.bind(response, query
            .useDefaultGroup()
            .tagKeyCount(new TagKeyCount.Request(c.getFilter(), c.getRange(), c.getLimit(),
                OptionalLimit.of(10))));
    }

    @POST
    @Path("key-suggest")
    public void keySuggest(
        @Suspended final AsyncResponse response, final MetadataKeySuggest request
    ) {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange,
            () -> OptionalLimit.of(request.getLimit().orElse(MetadataKeySuggest.DEFAULT_LIMIT)));

        final MatchOptions match = request
            .getMatch()
            .map(MatchOptions.Builder::build)
            .orElse(MetadataKeySuggest.DEFAULT_MATCH);

        httpAsync.bind(response, query
            .useDefaultGroup()
            .keySuggest(new KeySuggest.Request(c.getFilter(), c.getRange(), c.getLimit(), match,
                request.getKey())));
    }

    @POST
    @Path("tag-suggest")
    public void tagSuggest(
        @Suspended final AsyncResponse response, final MetadataTagSuggest request
    ) {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange,
            () -> OptionalLimit.of(request.getLimit().orElse(MetadataTagSuggest.DEFAULT_LIMIT)));

        final MatchOptions match = request
            .getMatch()
            .map(MatchOptions.Builder::build)
            .orElse(MetadataTagSuggest.DEFAULT_MATCH);

        httpAsync.bind(response, query
            .useDefaultGroup()
            .tagSuggest(new TagSuggest.Request(c.getFilter(), c.getRange(), c.getLimit(), match,
                request.getKey(), request.getValue())));
    }

    @POST
    @Path("tag-value-suggest")
    public void tagValueSuggest(
        @Suspended final AsyncResponse response, final MetadataTagValueSuggest request
    ) {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange,
            () -> OptionalLimit.of(
                request.getLimit().orElse(MetadataTagValueSuggest.DEFAULT_LIMIT)));

        httpAsync.bind(response, query
            .useDefaultGroup()
            .tagValueSuggest(new TagValueSuggest.Request(c.getFilter(), c.getRange(), c.getLimit(),
                request.getKey())));
    }

    @POST
    @Path("tag-values-suggest")
    public void tagValuesSuggest(
        @Suspended final AsyncResponse response, final MetadataTagValuesSuggest request
    ) {
        final RequestCriteria c = toCriteria(request::getFilter, request::getRange,
            () -> OptionalLimit.of(
                request.getLimit().orElse(MetadataTagValuesSuggest.DEFAULT_LIMIT)));

        final OptionalLimit groupLimit =
            request.getGroupLimit().map(OptionalLimit::of).orElseGet(OptionalLimit::empty);

        final List<String> exclude = request.getExclude().orElseGet(ImmutableList::of);

        httpAsync.bind(response, query
            .useDefaultGroup()
            .tagValuesSuggest(
                new TagValuesSuggest.Request(c.getFilter(), c.getRange(), c.getLimit(), groupLimit,
                    exclude)));
    }

    private RequestCriteria toCriteria(
        final Supplier<Optional<Filter>> optionalFilter,
        final Supplier<Optional<QueryDateRange>> optionalRange
    ) {
        return toCriteria(optionalFilter, optionalRange, OptionalLimit::empty);
    }

    private RequestCriteria toCriteria(
        final Supplier<Optional<Filter>> optionalFilter,
        final Supplier<Optional<QueryDateRange>> optionalRange, final Supplier<OptionalLimit> limit
    ) {
        final long now = clock.currentTimeMillis();
        final Filter c = optionalFilter.get().orElseGet(TrueFilter::get);
        final DateRange range = optionalRange
            .get()
            .map(r -> r.buildDateRange(now))
            .orElseGet(
                () -> new DateRange(now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS), now));

        return new RequestCriteria(c, range, limit.get());
    }

    @Data
    static class RequestCriteria {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
    }
}
