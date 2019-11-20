/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.cluster

import com.spotify.heroic.common.UsableGroupManager
import eu.toolchain.async.AsyncFuture
import java.net.URI
import java.util.*
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Predicate

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
interface ClusterManager: UsableGroupManager<MutableList<ClusterShard>> {
    data class Statistics(val onlineNodes: Int, val offlineNodes: Int)

    /**
     * Get the list of static nodes.
     */
    fun getStaticNodes(): AsyncFuture<Set<URI>>

    /**
     * Remove a static node.
     */
    fun removeStaticNode(node: URI): AsyncFuture<Void>

    /**
     * Add a static node.
     */
    fun addStaticNode(node: URI): AsyncFuture<Void>

    /**
     * Eventually consistent view of the currently known nodes in the cluster
     */
     fun getNodes(): List<ClusterNode>

    /**
     * Eventually consistent view of the currently known nodes in a specific shard
     */
    fun getNodesForShard(shard: Map<String, String>): List<ClusterNode>

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
    fun <T> withNodeInShardButNotWithId(
        shard: Map<String, String>, exclude: Predicate<ClusterNode>,
        registerNodeUse: Consumer<ClusterNode>, fn: Function<ClusterNode.Group, T>
    ): Optional<NodeResult<T>>

    /**
     * Check if there's at this instant any more nodes in the specified shard.
     * I.e. is a call to withNodeInShardButNotWithId(...) likely to succeed?
     *
     * NOTE: This call is eventually consistent. The node registry might change before a call to
     * withNodeInShardButNotWithId(...), so the result of this method should only be used as an
     * advice. A call to withNodeInShardButNotWithId could still fail because of no node available.
     */
    fun hasNextButNotWithId(shard: Map<String, String>, exclude: Predicate<ClusterNode>): Boolean

    /**
     * Perform a refresh of the cluster information.
     *
     * @return a future indicating the state of the refresh
     */
    fun refresh(): AsyncFuture<Void>

    fun getStatistics(): Statistics?

    fun protocols(): Set<RpcProtocol>

    /**
     * The result of applying a function to a random node in a shard
     */
    data class NodeResult<T>(
        val returnValue: T,

        /* NOTE: No guarantees are made about this object having a live connection. Only made
         * available to facilitate identifying which node that was used. */
        val node: ClusterNode
    )
}