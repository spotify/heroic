package com.spotify.heroic.cluster.model;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Data;

import com.spotify.heroic.cluster.NodeCapability;

@Data
public class NodeMetadata {
    public static final Set<NodeCapability> DEFAULT_CAPABILITIES = new HashSet<>();
    static {
        DEFAULT_CAPABILITIES.add(NodeCapability.QUERY);
    }

    private final int version;
    private final UUID id;
    private final Map<String, String> tags;
    private final Set<NodeCapability> capabilities;

    /**
     * Checks if both the given tags and capability matches.
     */
    public boolean matches(Map<String, String> tags, NodeCapability capability) {
        if (!matchesTags(tags))
            return false;

        if (!matchesCapability(capability))
            return false;

        return true;
    }

    /**
     * Capabilities match if the node capabilities are not set, or if it is set
     * and contains the specified value.
     *
     * @param capability
     *            The capability to match, or <code>null</code> for any
     *            capability.
     * @return <code>bool</code> indicating if the capabiltiy matches or not.
     */
    public boolean matchesCapability(NodeCapability capability) {
        if (this.capabilities == null || capability == null)
            return true;

        return capabilities.contains(capability);
    }

    /**
     * Checks if the set of 'other' tags matches the tags of this meta data.
     */
    public boolean matchesTags(Map<String, String> tags) {
        if (this.tags == null)
            return true;

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
