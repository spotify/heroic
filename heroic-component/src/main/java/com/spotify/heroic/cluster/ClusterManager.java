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

import java.net.URI;
import java.util.List;
import java.util.Map;

import com.spotify.heroic.common.LifeCycle;

import eu.toolchain.async.AsyncFuture;
import lombok.Data;

/**
 * Handles management of cluster state.
 *
 * The primary responsibility is to receive refresh requests through {@link #refresh()} that should cause the cluster
 * state to be updated.
 *
 * It also provides an interface for looking up nodes through {@link #findNode(Map, NodeCapability)}.
 *
 * @author udoprog
 */
public interface ClusterManager extends LifeCycle {
    @Data
    public static final class Statistics {
        private final int onlineNodes;
        private final int offlineNodes;
    }

    /**
     * Add a static node, mainly used for testing.
     */
    public AsyncFuture<Void> addStaticNode(URI node);

    public List<NodeRegistryEntry> getNodes();

    public AsyncFuture<Void> refresh();

    public Statistics getStatistics();

    public ClusterNodeGroup useDefaultGroup();

    public ClusterNodeGroup useGroup(String group);

    /**
     * Future that will be resolved, after the cluster manager has been fully initialized.
     */
    public AsyncFuture<Void> initialized();
}
