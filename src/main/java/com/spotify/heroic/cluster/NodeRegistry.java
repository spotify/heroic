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
    private static final Random random = new Random(System.currentTimeMillis());

    public NodeRegistryEntry findEntry(Map<String, String> tags) {
        final List<NodeRegistryEntry> matches = new ArrayList<>();

        for (final NodeRegistryEntry entry : entries) {
            if (tagsMatch(entry.getMetadata().getTags(), tags)) {
                matches.add(entry);
            }
        }

        if (matches.isEmpty()) {
            return null;
        }

        return matches.get(random.nextInt(matches.size()));
    }

    /**
     * Checks whether all the entries in query are equal to the corresponding
     * entries in tags. Obviously tags could have more entries.
     * 
     * @param query
     * @param tags
     * @return
     */
    private boolean tagsMatch(Map<String, String> query,
            Map<String, String> tags) {
        for (final Map.Entry<String, String> entry : query.entrySet()) {
            final String entryValue = entry.getValue();
            final String tagValue = tags.get(entry.getKey());

            if (tagValue == null) {
                if (entryValue == null) {
                    continue;
                }

                return false;
            }

            if (!tagValue.equals(entryValue)) {
                return false;
            }
        }

        return true;
    }
}