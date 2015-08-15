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

package com.spotify.heroic.rpc.httprpc;

import java.net.URI;

import javax.inject.Inject;

import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.httpclient.HttpClientManager;
import com.spotify.heroic.httpclient.HttpClientSession;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

public class HttpRpcProtocol implements RpcProtocol {
    @Inject
    private AsyncFramework async;

    @Inject
    private HttpClientManager clients;

    @Override
    public AsyncFuture<ClusterNode> connect(final URI uri) {
        final HttpClientSession client = clients.newSession(uri, "rpc");

        final Transform<HttpRpcMetadata, ClusterNode> transform = new Transform<HttpRpcMetadata, ClusterNode>() {
            @Override
            public ClusterNode transform(final HttpRpcMetadata r) throws Exception {
                final NodeMetadata m = new NodeMetadata(r.getVersion(), r.getId(), r.getTags(), r.getCapabilities());
                return buildClusterNode(m);
            }

            /**
             * Pick the best cluster node implementation depending on the provided metadata.
             */
            private ClusterNode buildClusterNode(NodeMetadata m) throws Exception {
                /* Only create a client for the highest possibly supported version. */
                final HttpClientSession client = clients.newSession(uri, "rpc");
                return new HttpRpcResource.HttpRpcClusterNode(async, uri, client, m);
            }
        };

        return client.get(HttpRpcMetadata.class, "metadata").transform(transform);
    }
}