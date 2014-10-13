package com.spotify.heroic.cluster.model;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface NodeMetadata {
    /**
     * Checks if both the given tags and capability matches.
     */
    public boolean matches(Map<String, String> tags, NodeCapability capability);

    /**
     * Capabilities match if the node capabilities are not set, or if it is set and contains the specified value.
     *
     * @param capability
     *            The capability to match, or <code>null</code> for any capability.
     * @return <code>bool</code> indicating if the capabiltiy matches or not.
     */
    public boolean matchesCapability(NodeCapability capability);

    /**
     * Checks if the set of 'other' tags matches the tags of this meta data.
     */
    public boolean matchesTags(Map<String, String> tags);

    public UUID getId();

    public int getVersion();

    public Map<String, String> getTags();

    public Set<NodeCapability> getCapabilities();
}
