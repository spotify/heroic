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

import java.net.URI;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.NodeRegistryEntry;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.JavaxRestFramework.Resume;
import com.spotify.heroic.http.DataResponse;

import eu.toolchain.async.AsyncFuture;

@Path("/cluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    @Inject
    private JavaxRestFramework httpAsync;

    @Inject
    private ClusterManager cluster;

    /**
     * Encode/Decode functions, helpful when interacting with cassandra through cqlsh.
     */
    @GET
    @Path("/status")
    public Response status() {
        final List<ClusterNodeStatus> nodes = convert(cluster.getNodes());
        final ClusterStatus status = new ClusterStatus(nodes, cluster.getStatistics());
        return Response.status(Response.Status.OK).entity(status).build();
    }

    private List<ClusterNodeStatus> convert(List<NodeRegistryEntry> nodes) {
        return ImmutableList.copyOf(nodes.stream().map(this::convert).iterator());
    }

    private ClusterNodeStatus convert(NodeRegistryEntry e) {
        final NodeMetadata m = e.getMetadata();

        return new ClusterNodeStatus(e.getClusterNode().toString(), m.getId(), m.getVersion(),
                m.getTags(), m.getCapabilities());
    }

    private static final Resume<Void, DataResponse<Boolean>> ADD_NODE =
            new Resume<Void, DataResponse<Boolean>>() {
                @Override
                public DataResponse<Boolean> resume(Void value) throws Exception {
                    return new DataResponse<>(true);
                }
            };

    @POST
    @Path("/nodes")
    public void addNode(@Suspended AsyncResponse response, URI uri) {
        AsyncFuture<Void> callback = cluster.addStaticNode(uri);
        httpAsync.bind(response, callback, ADD_NODE);
    }
}
