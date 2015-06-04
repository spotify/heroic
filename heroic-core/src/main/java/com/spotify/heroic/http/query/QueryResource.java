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

import lombok.Data;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.metric.ClusteredMetricManager;
import com.spotify.heroic.metric.MetricQuery;
import com.spotify.heroic.metric.MetricQueryBuilder;
import com.spotify.heroic.metric.MetricResult;
import com.spotify.heroic.metric.model.ShardedResultGroups;
import com.spotify.heroic.utils.HttpAsyncUtils;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {
    private static final class MetricsResumer implements HttpAsyncUtils.Resume<MetricResult, QueryMetricsResponse> {
        @Override
        public QueryMetricsResponse resume(MetricResult result) throws Exception {
            final ShardedResultGroups groups = result.getMetricGroups();
            return new QueryMetricsResponse(result.getQueryRange(), groups.getGroups(), groups.getStatistics(),
                    groups.getErrors(), groups.getLatencies());
        }
    };

    private static final MetricsResumer METRICS = new MetricsResumer();

    @Inject
    private HttpAsyncUtils httpAsync;

    @Inject
    private ClusteredMetricManager metrics;

    @Inject
    private ClusterManager cluster;

    private final Cache<UUID, StreamQuery> streamQueries = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES).<UUID, StreamQuery> build();

    @POST
    @Path("/metrics/stream")
    public List<StreamId> metricsStream(@QueryParam("backend") String backendGroup, QueryMetrics query) {
        final MetricQuery request = setupBuilder(backendGroup, query).build();

        final Collection<NodeRegistryEntry> nodes = cluster.findAllShards(NodeCapability.QUERY);
        final List<StreamId> ids = new ArrayList<>();

        for (NodeRegistryEntry node : nodes) {
            final UUID id = UUID.randomUUID();
            streamQueries.put(id, new StreamQuery(node, request));
            ids.add(new StreamId(node.getMetadata().getTags(), id));
        }

        return ids;
    }

    @POST
    @Path("/metrics/stream/{id}")
    public void metricsStreamId(@Suspended final AsyncResponse response, @PathParam("id") final UUID id) {
        if (id == null)
            throw new BadRequestException("Id must be a valid UUID");

        final StreamQuery query = streamQueries.getIfPresent(id);

        if (query == null)
            throw new NotFoundException("Stream query not found with id: " + id);

        final AsyncFuture<MetricResult> callback = metrics.queryOnNode(query.getRequest(), query.getNode());

        callback.on(new FutureDone<Object>() {
            @Override
            public void failed(Throwable cause) throws Exception {
            }

            @Override
            public void resolved(Object result) throws Exception {
                // invalidate on successful response to free up resources quicker.
                streamQueries.invalidate(id);
            }

            @Override
            public void cancelled() throws Exception {
            }
        });

        response.setTimeout(300, TimeUnit.SECONDS);
        httpAsync.handleAsyncResume(response, callback, METRICS);
    }

    @POST
    @Path("/metrics")
    public void metrics(@Suspended final AsyncResponse response, @QueryParam("backend") String backendGroup,
            QueryMetrics query) {
        final MetricQuery request = setupBuilder(backendGroup, query).build();

        final AsyncFuture<MetricResult> callback = metrics.query(request);

        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.handleAsyncResume(response, callback, METRICS);
    }

    @SuppressWarnings("deprecation")
    private MetricQueryBuilder setupBuilder(String backendGroup, QueryMetrics query) {
        return metrics.newRequest().key(query.getKey()).tags(query.getTags()).groupBy(query.getGroupBy())
                .backendGroup(backendGroup).queryString(query.getQuery()).filter(query.getFilter())
                .range(query.getRange().buildDateRange()).disableCache(query.isNoCache())
                .aggregation(query.makeAggregation()).source(query.getSource());
    }

    @Data
    public static final class StreamId {
        private final Map<String, String> tags;
        private final UUID id;
    }

    @Data
    private static final class StreamQuery {
        private final NodeRegistryEntry node;
        private final MetricQuery request;
    }
}
