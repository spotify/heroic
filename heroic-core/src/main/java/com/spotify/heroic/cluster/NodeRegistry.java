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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import eu.toolchain.async.AsyncFramework;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Data;

@Data
public class NodeRegistry {
    private static final Random random = new Random();

    private final AsyncFramework async;
    private final List<ClusterNode> entries;
    private final int totalNodes;

    private Multimap<Map<String, String>, ClusterNode> buildShards(
        List<ClusterNode> entries
    ) {
        final Multimap<Map<String, String>, ClusterNode> shards = LinkedListMultimap.create();

        for (final ClusterNode e : entries) {
            shards.put(e.metadata().getTags(), e);
        }

        return shards;
    }

    public List<ClusterNode> getEntries() {
        return ImmutableList.copyOf(entries);
    }

    public int getOnlineNodes() {
        return entries.size();
    }

    public int getOfflineNodes() {
        return totalNodes - entries.size();
    }

    public Set<Map<String, String>> getShards() {
        return buildShards(entries).keySet();
    }

    public List<ClusterNode> getNodesInShard(final Map<String, String> shard) {
        final Multimap<Map<String, String>, ClusterNode> shardToNode = buildShards(entries);

        final Collection<ClusterNode> nodesInShard = shardToNode.get(shard);
        final List<ClusterNode> result = Lists.newArrayList();
        result.addAll(nodesInShard);

        return result;
    }

    public Optional<ClusterNode> getNodeInShardButNotWithId(
        final Map<String, String> shard, final Predicate<ClusterNode> exclude
    ) {
        final Multimap<Map<String, String>, ClusterNode> shardToNode = buildShards(entries);

        final Collection<ClusterNode> nodesInShard = shardToNode.get(shard);

        final List<ClusterNode> randomizedList =
            nodesInShard.stream().filter(ClusterNode::isAlive).collect(Collectors.toList());
        Collections.shuffle(randomizedList, random);

        for (final ClusterNode n : randomizedList) {
            if (exclude.test(n)) {
                continue;
            }
            return Optional.of(n);
        }

        return Optional.empty();
    }
}
