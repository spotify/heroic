package com.spotify.heroic.cluster;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Data;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;

public interface ClusterManager {
    @Data
    public static final class Statistics {
        private final int onlineNodes;
        private final int offlineNodes;
    }

    /**
     * Sooo, why invent another null value?
     *
     * Mainly because GuiceIntoHK2Bridge does not support injection of null
     * values in resources.
     *
     * @author udoprog
     */
    public static final class Null implements ClusterManager {
        private Null() {
        }

        @Override
        public UUID getLocalNodeId() {
            throw new NullPointerException();
        }

        @Override
        public Map<String, String> getLocalNodeTags() {
            throw new NullPointerException();
        }

        @Override
        public NodeRegistryEntry findNode(Map<String, String> tags,
                NodeCapability capability) {
            throw new NullPointerException();
        }

        @Override
        public Callback<Void> refresh() {
            throw new NullPointerException();
        }

        @Override
        public Statistics getStatistics() {
            throw new NullPointerException();
        }

        @Override
        public Set<NodeCapability> getCapabilities() {
            throw new NullPointerException();
        }
    }

    public static final ClusterManager NULL = new Null();

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
