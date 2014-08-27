package com.spotify.heroic.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import lombok.Data;

import com.spotify.heroic.cluster.model.NodeRegistryEntry;

@Data
public class NodeRegistry {
    private final List<NodeRegistryEntry> entries;
    private final int totalNodes;
    private static final Random random = new Random(System.currentTimeMillis());

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

        return matches.get(random.nextInt(matches.size()));
    }

    public int getOnlineNodes() {
        return entries.size();
    }

    public int getOfflineNodes() {
        return totalNodes - entries.size();
    }
}