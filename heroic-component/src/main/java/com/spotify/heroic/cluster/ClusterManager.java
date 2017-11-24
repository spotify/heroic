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
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Data;

/**
 * Handles management of cluster state.
 * <p>
 * The primary responsibility is to receive refresh requests through {@link #refresh()} that should
 * cause the cluster state to be updated.
 * <p>
 * It also provides an interface for applying a function on a node in a shard, via
 * {@link #withNodeInShardButNotWithId(shardTags, excludeIds, registerNodeUse, fn)}. *
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

    /**
     * Eventually consistent view of the currently known nodes in the cluster
     */
    List<ClusterNode> getNodes();

    /**
     * Eventually consistent view of the currently known nodes in a specific shard
     */
    List<ClusterNode> getNodesForShard(Map<String, String> shard);

    /**
     * Run function on one random ClusterNode in a specific shard. The caller is allowed the
     * possibility to decide which nodes that would be suitable.
     *
     * @param shard the shard
     * @param exclude predicate that can decide if a ClusterNode would be suitable to use
     * @param registerNodeUse consumer that is called when a ClusterNode is about to be used
     * @param fn the function to apply on the node
     * @param <T> type of the return value
     * @return Return value of the applied function, Id of the node, string representation of node
     */
    <T> Optional<NodeResult<T>> withNodeInShardButNotWithId(
        Map<String, String> shard, Predicate<ClusterNode> exclude,
        Consumer<ClusterNode> registerNodeUse, Function<ClusterNode.Group, T> fn
    );

    /**
     * Check if there's at this instant any more nodes in the specified shard.
     * I.e. is a call to withNodeInShardButNotWithId(...) likely to succeed?
     *
     * NOTE: This call is eventually consistent. The node registry might change before a call to
     * withNodeInShardButNotWithId(...), so the result of this method should only be used as an
     * advice. A call to withNodeInShardButNotWithId could still fail because of no node available.
     */
    boolean hasNextButNotWithId(Map<String, String> shard, Predicate<ClusterNode> exclude);

    /**
     * Perform a refresh of the cluster information.
     *
     * @return a future indicating the state of the refresh
     */
    AsyncFuture<Void> refresh();

    Statistics getStatistics();

    Set<RpcProtocol> protocols();

    /**
     * The result of applying a function to a random node in a shard
     */
    @Data
    class NodeResult<T> {
        private final T returnValue;

        /* NOTE: No guarantees are made about this object having a live connection. Only made
         * available to facilitate identifying which node that was used. */
        private final ClusterNode node;
    }
}
