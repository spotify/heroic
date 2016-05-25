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

package com.spotify.heroic.http.query;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryBuilder;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.metric.QueryResult;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Path("query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {
    private final JavaxRestFramework httpAsync;
    private final QueryManager query;
    private final AsyncFramework async;

    @Inject
    public QueryResource(JavaxRestFramework httpAsync, QueryManager query, AsyncFramework async) {
        this.httpAsync = httpAsync;
        this.query = query;
        this.async = async;
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.TEXT_PLAIN)
    public void metricsText(
        @Suspended final AsyncResponse response, @QueryParam("group") String group, String query
    ) {
        final Query q = this.query.newQueryFromString(query).build();

        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));
        final AsyncFuture<QueryResult> callback = g.query(q);

        bindMetricsResponse(response, callback);
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.APPLICATION_JSON)
    public void metrics(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        QueryMetrics query
    ) {
        final Query q = setupQuery(query).build();

        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));
        final AsyncFuture<QueryResult> callback = g.query(q);

        bindMetricsResponse(response, callback);
    }

    @POST
    @Path("batch")
    public void metrics(
        @Suspended final AsyncResponse response, @QueryParam("backend") String group,
        final QueryBatch query
    ) {
        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));

        final List<AsyncFuture<Pair<String, QueryResult>>> futures = new ArrayList<>();

        for (final Map.Entry<String, QueryMetrics> e : query.getQueries().entrySet()) {
            final Query q = setupQuery(e.getValue()).rangeIfAbsent(query.getRange()).build();
            futures.add(g.query(q).directTransform(r -> Pair.of(e.getKey(), r)));
        }

        final AsyncFuture<QueryBatchResponse> future =
            async.collect(futures).directTransform(entries -> {
                final ImmutableMap.Builder<String, QueryMetricsResponse> results =
                    ImmutableMap.builder();

                for (final Pair<String, QueryResult> e : entries) {
                    final QueryResult r = e.getRight();
                    results.put(e.getLeft(),
                        new QueryMetricsResponse(r.getRange(), r.getGroups(), r.getErrors(),
                            r.getTrace()));
                }

                return new QueryBatchResponse(results.build());
            });

        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.bind(response, future);
    }

    private void bindMetricsResponse(
        final AsyncResponse response, final AsyncFuture<QueryResult> callback
    ) {
        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.bind(response, callback,
            r -> new QueryMetricsResponse(r.getRange(), r.getGroups(), r.getErrors(),
                r.getTrace()));
    }

    @SuppressWarnings("deprecation")
    private QueryBuilder setupQuery(final QueryMetrics q) {
        Supplier<? extends QueryBuilder> supplier = () -> {
            return query
                .newQuery()
                .key(q.getKey())
                .tags(q.getTags())
                .groupBy(q.getGroupBy())
                .filter(q.getFilter())
                .range(q.getRange())
                .aggregation(q.getAggregation())
                .source(q.getSource())
                .options(q.getOptions());
        };

        return q
            .getQuery()
            .map(query::newQueryFromString)
            .orElseGet(supplier)
            .rangeIfAbsent(q.getRange())
            .optionsIfAbsent(q.getOptions())
            .features(q.getFeatures());
    }

    @Data
    public static final class StreamId {
        private final Map<String, String> tags;
        private final UUID id;
    }

    @Data
    private static final class StreamQuery {
        private final QueryManager.Group group;
        private final Query query;
    }
}
