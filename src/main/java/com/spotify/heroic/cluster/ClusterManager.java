package com.spotify.heroic.cluster;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Data;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.injection.LifeCycle;

public interface ClusterManager extends LifeCycle {
    @Data
    public static final class Statistics {
        private final int onlineNodes;
        private final int offlineNodes;
    }

    public UUID getLocalNodeId();

    public Map<String, String> getLocalNodeTags();

    /**
     * Find a node that matches the given tags and capability.
     *
     * @param tags
     *            The tags to match.
     * @param capability
     *            The capability to match (may be <code>null</code>).
     * @return A NodeRegistryEntry matching the parameters or <code>null</code>
     *         if none matches.
     */
    public NodeRegistryEntry findNode(final Map<String, String> tags,
            NodeCapability capability);

    public Callback<Void> refresh();

    public Statistics getStatistics();

    public Set<NodeCapability> getCapabilities();
}
