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

package com.spotify.heroic.http.cluster;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.JavaxRestFramework.Resume;
import com.spotify.heroic.http.DataResponse;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.List;

@Path("/cluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    private final JavaxRestFramework httpAsync;
    private final ClusterManager cluster;

    @Inject
    public ClusterResource(final JavaxRestFramework httpAsync, final ClusterManager cluster) {
        this.httpAsync = httpAsync;
        this.cluster = cluster;
    }

    /**
     * Encode/Decode functions, helpful when interacting with cassandra through cqlsh.
     */
    @GET
    @Path("status")
    public Response status() {
        final List<ClusterNodeStatus> nodes = convert(cluster.getNodes());
        final ClusterStatus status = new ClusterStatus(nodes, cluster.getStatistics());
        return Response.status(Response.Status.OK).entity(status).build();
    }

    private List<ClusterNodeStatus> convert(List<ClusterNode> nodes) {
        return ImmutableList.copyOf(nodes.stream().map(this::convert).iterator());
    }

    private ClusterNodeStatus convert(ClusterNode node) {
        final NodeMetadata m = node.metadata();

        return new ClusterNodeStatus(node.toString(), m.getId(), m.getVersion(), m.getTags());
    }

    private static final Resume<Void, DataResponse<Boolean>> OK =
        (Void value) -> new DataResponse<>(true);

    @GET
    @Path("nodes")
    public void getNodes(@Suspended AsyncResponse response, URI uri) {
        httpAsync.bind(response, cluster.getStaticNodes());
    }

    @DELETE
    @Path("nodes")
    public void removeNode(@Suspended AsyncResponse response, URI uri) {
        httpAsync.bind(response, cluster.removeStaticNode(uri), OK);
    }

    @POST
    @Path("nodes")
    public void addNode(@Suspended AsyncResponse response, URI uri) {
        httpAsync.bind(response, cluster.addStaticNode(uri), OK);
    }

    @POST
    @Path("refresh")
    public void refresh(@Suspended AsyncResponse response) {
        httpAsync.bind(response, cluster.refresh());
    }
}
