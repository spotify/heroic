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

package com.spotify.heroic.cluster;

import com.spotify.heroic.common.UsableGroupManager;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles management of cluster state.
 * <p>
 * The primary responsibility is to receive refresh requests through {@link #refresh()} that should
 * cause the cluster state to be updated.
 * <p>
 * It also provides an interface for looking up nodes through {@link #findNode(Map, NodeCapability)}
 * .
 *
 * @author udoprog
 */
public interface ClusterManager extends UsableGroupManager<List<ClusterShard>> {
    @Data
    class Statistics {
        private final int onlineNodes;
        private final int offlineNodes;
    }

    /**
     * Get the list of static nodes.
     */
    AsyncFuture<Set<URI>> getStaticNodes();

    /**
     * Remove a static node.
     */
    AsyncFuture<Void> removeStaticNode(URI node);

    /**
     * Add a static node.
     */
    AsyncFuture<Void> addStaticNode(URI node);

    List<ClusterNode> getNodes();

    /**
     * Perform a refresh of the cluster information.
     *
     * @return a future indicating the state of the refresh
     */
    AsyncFuture<Void> refresh();

    Statistics getStatistics();

    Set<RpcProtocol> protocols();
}
