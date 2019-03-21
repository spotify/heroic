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

package com.spotify.heroic.http.status;

import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.common.ServiceInfo;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricManager;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/status")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class StatusResource {
    private final Set<Consumer> consumers;
    private final MetricManager metric;
    private final MetadataManager metadata;
    private final ClusterManager cluster;
    private final ServiceInfo service;

    @Inject
    public StatusResource(
        final Set<Consumer> consumers, final MetricManager metric, final MetadataManager metadata,
        final ClusterManager cluster, final ServiceInfo service
    ) {
        this.consumers = consumers;
        this.metric = metric;
        this.metadata = metadata;
        this.cluster = cluster;
        this.service = service;
    }

    @GET
    public Response get() {
        final StatusResponse.Consumer consumers = buildConsumerStatus();
        final StatusResponse.Backend backends = buildBackendStatus();
        final StatusResponse.MetadataBackend metadataBackends = buildMetadataBackendStatus();

        final StatusResponse.Cluster cluster = buildClusterStatus();

        final boolean allOk =
            consumers.isOk() && backends.isOk() && metadataBackends.isOk() && cluster.isOk();

        final StatusResponse response =
            new StatusResponse(service, allOk, consumers, backends, metadataBackends, cluster);

        if (!response.isOk()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(response).build();
        }

        return Response.status(Response.Status.OK).entity(response).build();
    }

    private StatusResponse.Cluster buildClusterStatus() {
        final ClusterManager.Statistics s = cluster.getStatistics();

        if (s == null) {
            return new StatusResponse.Cluster(true, 0, 0);
        }

        final boolean ok = s.getOfflineNodes() == 0 || s.getOnlineNodes() > 0;
        return new StatusResponse.Cluster(ok, s.getOnlineNodes(), s.getOfflineNodes());
    }

    private StatusResponse.Backend buildBackendStatus() {
        final Set<GroupMember<MetricBackend>> members = metric.groupSet().inspectAll();
        final int available = members.size();
        int ready = (int) members.stream().filter(b -> b.getMember().isReady()).count();
        return new StatusResponse.Backend(available == ready, available, ready);
    }

    private StatusResponse.Consumer buildConsumerStatus() {
        final int available = consumers.size();

        int ready = 0;
        long errors = 0;
        long consumingThreads = 0;
        long totalThreads = 0;
        boolean allOk = true;

        for (final Consumer consumer : consumers) {
            if (consumer.isReady()) {
                ready += 1;
                final Statistics s = consumer.getStatistics();

                final long consuming = s.get(Consumer.CONSUMING, 0);
                final long total = s.get(Consumer.TOTAL, 0);

                errors += s.get(Consumer.ERRORS, 0);
                consumingThreads += consuming;
                totalThreads += total;

                // OK if all threads configured are actively consuming.
                allOk = allOk && (consuming == total);
            }
        }

        return new StatusResponse.Consumer((available == ready) && allOk, available, ready, errors,
            consumingThreads, totalThreads);
    }

    private StatusResponse.MetadataBackend buildMetadataBackendStatus() {
        final Set<GroupMember<MetadataBackend>> members = metadata.groupSet().inspectAll();
        final int available = members.size();
        int ready = (int) members.stream().map(b -> b.getMember().isReady()).count();
        return new StatusResponse.MetadataBackend(available == ready, available, ready);
    }
}
