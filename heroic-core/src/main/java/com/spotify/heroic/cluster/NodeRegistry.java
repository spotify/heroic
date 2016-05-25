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
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

@Data
public class NodeRegistry {
    private static final Random random = new Random(System.currentTimeMillis());

    private final AsyncFramework async;
    private final List<ClusterNode> entries;
    private final int totalNodes;

    private Multimap<Map<String, String>, ClusterNode> buildShards(
        List<ClusterNode> entries, NodeCapability capability
    ) {
        final Multimap<Map<String, String>, ClusterNode> shards = LinkedListMultimap.create();

        for (final ClusterNode e : entries) {
            if (!e.metadata().matchesCapability(capability)) {
                continue;
            }

            shards.put(e.metadata().getTags(), e);
        }

        return shards;
    }

    public List<ClusterNode> getEntries() {
        return ImmutableList.copyOf(entries);
    }

    /**
     * Find an entry that matches the given tags depending on its metadata.
     *
     * @param tags The tags to match.
     * @return A random matching entry.
     */
    public ClusterNode findEntry(Map<String, String> tags, NodeCapability capability) {
        final List<ClusterNode> matches = new ArrayList<>();

        for (final ClusterNode entry : entries) {
            if (entry.metadata().matches(tags, capability)) {
                matches.add(entry);
            }
        }

        if (matches.isEmpty()) {
            return null;
        }

        if (matches.size() == 1) {
            return matches.get(0);
        }

        return matches.get(random.nextInt(matches.size()));
    }

    public int getOnlineNodes() {
        return entries.size();
    }

    public int getOfflineNodes() {
        return totalNodes - entries.size();
    }

    public List<ClusterNode> findAllShards(NodeCapability capability) {
        final List<ClusterNode> result = Lists.newArrayList();

        final Multimap<Map<String, String>, ClusterNode> shards = buildShards(entries, capability);

        final Set<Entry<Map<String, String>, Collection<ClusterNode>>> entries =
            shards.asMap().entrySet();

        for (final Entry<Map<String, String>, Collection<ClusterNode>> e : entries) {
            final ClusterNode one = pickOne(e.getValue());

            if (one == null) {
                continue;
            }

            result.add(one);
        }

        return result;
    }

    /**
     * Find multiple registry entries from all shards.
     *
     * @param capability Capability to find.
     * @param n Max number of entries to find.
     * @return An iterable of iterables, containing all found entries.
     */
    public List<Pair<Map<String, String>, List<ClusterNode>>> findManyFromAllShards(
        NodeCapability capability, int n
    ) {
        final List<Pair<Map<String, String>, List<ClusterNode>>> result = Lists.newArrayList();

        final Multimap<Map<String, String>, ClusterNode> shards = buildShards(entries, capability);

        final Set<Entry<Map<String, String>, Collection<ClusterNode>>> entries =
            shards.asMap().entrySet();

        for (final Entry<Map<String, String>, Collection<ClusterNode>> e : entries) {
            final List<ClusterNode> many = pickN(e.getValue(), n);

            if (many.isEmpty()) {
                continue;
            }

            result.add(Pair.of(e.getKey(), many));
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

    private List<ClusterNode> pickN(final Collection<ClusterNode> options, int n) {
        if (options.isEmpty()) {
            return ImmutableList.of();
        }

        final List<ClusterNode> entries = new ArrayList<>(options);

        Collections.shuffle(entries, random);

        if (options.size() <= n) {
            return ImmutableList.copyOf(options);
        }

        return entries.subList(0, n);
    }
}
