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
import com.spotify.heroic.common.OptionalLimit;
import eu.toolchain.async.AsyncFramework;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

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

    /**
     * Find multiple registry entries from all shards.
     *
     * @param n Max number of entries to find.
     * @return An iterable of iterables, containing all found entries.
     */
    public List<Pair<Map<String, String>, List<ClusterNode>>> findFromAllShards(
        OptionalLimit n
    ) {
        final List<Pair<Map<String, String>, List<ClusterNode>>> result = Lists.newArrayList();

        final Multimap<Map<String, String>, ClusterNode> shardToNode = buildShards(entries);

        final Set<Entry<Map<String, String>, Collection<ClusterNode>>> shardToNodeEntries =
            shardToNode.asMap().entrySet();

        for (final Entry<Map<String, String>, Collection<ClusterNode>> e : shardToNodeEntries) {
            result.add(Pair.of(e.getKey(), pickN(e.getValue(), n)));
        }

        return result;
    }

    private ClusterNode pickOne(Collection<ClusterNode> options) {
        if (options.isEmpty()) {
            return null;
        }

        final int selection = random.nextInt(options.size());

        if (options instanceof List) {
            final List<ClusterNode> list = (List<ClusterNode>) options;
            return list.get(selection);
        }

        int i = 0;

        for (final ClusterNode e : options) {
            if (i++ == selection) {
                return e;
            }
        }

        return null;
    }

    private List<ClusterNode> pickN(final Collection<ClusterNode> options, OptionalLimit n) {
        if (options.isEmpty()) {
            return ImmutableList.of();
        }

        final List<ClusterNode> entries =
            options.stream().filter(ClusterNode::isAlive).collect(Collectors.toList());

        Collections.shuffle(entries, random);

        return n.limitList(entries);
    }
}
