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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import lombok.Data;

@Data
public class NodeRegistry {
    private static final Random random = new Random(System.currentTimeMillis());

    private final AsyncFramework async;
    private final List<NodeRegistryEntry> entries;
    private final int totalNodes;

    private Multimap<Map<String, String>, NodeRegistryEntry> buildShards(List<NodeRegistryEntry> entries,
            NodeCapability capability) {
        final Multimap<Map<String, String>, NodeRegistryEntry> shards = LinkedListMultimap.create();

        for (final NodeRegistryEntry e : entries) {
            if (!e.getMetadata().matchesCapability(capability)) {
                continue;
            }

            shards.put(e.getMetadata().getTags(), e);
        }

        return shards;
    }

    public List<NodeRegistryEntry> getEntries() {
        return ImmutableList.copyOf(entries);
    }

    /**
     * Find an entry that matches the given tags depending on its metadata.
     *
     * @param tags The tags to match.
     * @return A random matching entry.
     */
    public NodeRegistryEntry findEntry(Map<String, String> tags, NodeCapability capability) {
        final List<NodeRegistryEntry> matches = new ArrayList<>();

        for (final NodeRegistryEntry entry : entries) {
            if (entry.getMetadata().matches(tags, capability))
                matches.add(entry);
        }

        if (matches.isEmpty())
            return null;

        if (matches.size() == 1)
            return matches.get(0);

        return matches.get(random.nextInt(matches.size()));
    }

    public int getOnlineNodes() {
        return entries.size();
    }

    public int getOfflineNodes() {
        return totalNodes - entries.size();
    }

    public Collection<NodeRegistryEntry> findAllShards(NodeCapability capability) {
        final List<NodeRegistryEntry> result = Lists.newArrayList();

        final Multimap<Map<String, String>, NodeRegistryEntry> shards = buildShards(entries, capability);

        for (final Entry<Map<String, String>, Collection<NodeRegistryEntry>> e : shards.asMap().entrySet()) {
            final NodeRegistryEntry one = pickOne(e.getValue());

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
    public Collection<Collection<NodeRegistryEntry>> findMultipleFromAllShards(NodeCapability capability, int n) {
        final Collection<Collection<NodeRegistryEntry>> result = Lists.newArrayList();

        final Multimap<Map<String, String>, NodeRegistryEntry> shards = buildShards(entries, capability);

        for (final Entry<Map<String, String>, Collection<NodeRegistryEntry>> e : shards.asMap().entrySet()) {
            final Collection<NodeRegistryEntry> many = pickN(e.getValue(), n);

            if (many.isEmpty()) {
                continue;
            }

            result.add(many);
        }

        return result;
    }

    private NodeRegistryEntry pickOne(Collection<NodeRegistryEntry> options) {
        if (options.isEmpty())
            return null;

        final int selection = random.nextInt(options.size());

        if (options instanceof List) {
            final List<NodeRegistryEntry> list = (List<NodeRegistryEntry>) options;
            return list.get(selection);
        }

        int i = 0;

        for (final NodeRegistryEntry e : options) {
            if (i++ == selection)
                return e;
        }

        return null;
    }

    private Collection<NodeRegistryEntry> pickN(Collection<NodeRegistryEntry> options, int n) {
        if (options.isEmpty()) {
            return Collections.emptySet();
        }

        final List<NodeRegistryEntry> entries = new ArrayList<>(options);

        Collections.shuffle(entries, random);

        if (options.size() <= n) {
            return options;
        }

        return entries.subList(0, n);
    }

    /**
     * Close all associated cluster nodes.
     *
     * @return Future indicating if all was closed successfully.
     */
    public AsyncFuture<Void> close() {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        for (final NodeRegistryEntry entry : entries)
            futures.add(entry.getClusterNode().close());

        return async.collect(futures, new Collector<Void, Void>() {
            @Override
            public Void collect(Collection<Void> results) throws Exception {
                return null;
            }
        });
    }
}