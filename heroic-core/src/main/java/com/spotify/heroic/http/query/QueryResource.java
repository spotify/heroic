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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryBuilder;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.ShardTrace;

import eu.toolchain.async.AsyncFuture;
import lombok.Data;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {
    @Inject
    private JavaxRestFramework httpAsync;

    @Inject
    private QueryManager query;

    @Inject
    private NodeMetadata localMetadata;

    private final Cache<UUID, StreamQuery> streamQueries = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES).<UUID, StreamQuery> build();

    @POST
    @Path("/metrics/stream")
    public List<StreamId> metricsStream(@QueryParam("backend") String backendGroup, QueryMetrics query) {
        final Query request = setupBuilder(query).build();

        final Collection<? extends QueryManager.Group> groups = this.query.useGroupPerNode(backendGroup);
        final List<StreamId> ids = new ArrayList<>();

        for (QueryManager.Group group : groups) {
            final UUID id = UUID.randomUUID();
            streamQueries.put(id, new StreamQuery(group, request));
            ids.add(new StreamId(group.first().node().metadata().getTags(), id));
        }

        return ids;
    }

    @POST
    @Path("/metrics/stream/{id}")
    public void metricsStreamId(@Suspended final AsyncResponse response, @PathParam("id") final UUID id) {
        if (id == null) {
            throw new BadRequestException("Id must be a valid UUID");
        }

        final StreamQuery streamQuery = streamQueries.getIfPresent(id);

        if (streamQuery == null) {
            throw new NotFoundException("Stream query not found with id: " + id);
        }

        final Query q = streamQuery.getQuery();
        final QueryManager.Group group = streamQuery.getGroup();
        final AsyncFuture<QueryResult> callback = group.query(q);

        callback.onResolved(r -> streamQueries.invalidate(id));

        bindMetricsResponse(response, callback);
    }

    @POST
    @Path("/metrics")
    public void metrics(@Suspended final AsyncResponse response, @QueryParam("backend") String backendGroup,
            QueryMetrics query) {
        final Query q = setupBuilder(query).build();

        final QueryManager.Group group = this.query.useGroup(backendGroup);
        final AsyncFuture<QueryResult> callback = group.query(q);

        bindMetricsResponse(response, callback);
    }

    private void bindMetricsResponse(final AsyncResponse response, final AsyncFuture<QueryResult> callback) {
        response.setTimeout(300, TimeUnit.SECONDS);

        final Stopwatch watch = Stopwatch.createStarted();

        httpAsync.bind(response, callback,
                (r) -> new QueryMetricsResponse(r.getRange(), r.getGroups(), r.getErrors(),
                        new ShardTrace("api", localMetadata, watch.elapsed(TimeUnit.MILLISECONDS), Statistics.empty(),
                                java.util.Optional.empty(), r.getTraces())));
    }

    @SuppressWarnings("deprecation")
    private QueryBuilder setupBuilder(final QueryMetrics q) {
        return q.getQuery().transform(query::newQueryFromString).or(() -> {
            return query.newQuery().key(q.getKey()).tags(q.getTags()).groupBy(q.getGroupBy()).filter(q.getFilter())
                    .range(Optional.of(q.getRange().buildDateRange())).aggregationQuery(q.getAggregators())
                    .source(q.getSource());
        });
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
