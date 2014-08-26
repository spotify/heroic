package com.spotify.heroic.cluster.model;

import java.util.Map;
import java.util.UUID;

import lombok.Data;

import com.spotify.heroic.cluster.DiscoveredClusterNode;

@Data
public class NodeMetadata {
    private final DiscoveredClusterNode discovered;
    private final int version;
    private final UUID id;
    private final Map<String, String> tags;

    /**
     * Checks weither the set of 'other' tags matches this metadata.
     *
     * @param other
     * @return
     */
    public boolean matches(Map<String, String> tags) {
        for (final Map.Entry<String, String> entry : this.tags.entrySet()) {
            final String value = entry.getValue();
            final String tagValue = tags.get(entry.getKey());

            if (tagValue == null) {
                if (value == null)
                    continue;

                return false;
            }

            if (!tagValue.equals(value))
                return false;
        }

        return true;
    }
}
