package com.spotify.heroic.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import lombok.Data;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;

@Data
public class NodeRegistry {
    private final List<NodeRegistryEntry> entries;
    private final int totalNodes;
    private final Multimap<Map<String, String>, NodeRegistryEntry> shards;
    private static final Random random = new Random(System.currentTimeMillis());

    public NodeRegistry(List<NodeRegistryEntry> entries, int totalNodes) {
        this.entries = entries;
        this.totalNodes = totalNodes;
        this.shards = buildShards(entries);
    }

    private Multimap<Map<String, String>, NodeRegistryEntry> buildShards(
            List<NodeRegistryEntry> entries) {

        final Multimap<Map<String, String>, NodeRegistryEntry> shards = LinkedListMultimap
                .create();

        for (final NodeRegistryEntry e : entries) {
            shards.put(e.getMetadata().getTags(), e);
        }

        return shards;
    }

    /**
     * Find an entry that matches the given tags depending on its metadata.
     *
     * @param tags
     *            The tags to match.
     * @return A random matching entry.
     */
    public NodeRegistryEntry findEntry(Map<String, String> tags,
            NodeCapability capability) {
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

        for (final Entry<Map<String, String>, Collection<NodeRegistryEntry>> e : shards
                .asMap().entrySet()) {
            final NodeRegistryEntry one = pickOne(e.getValue());

            if (one == null)
                continue;

            result.add(one);
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
}