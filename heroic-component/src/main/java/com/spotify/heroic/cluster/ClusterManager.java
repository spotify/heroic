package com.spotify.heroic.cluster;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.Data;

import com.spotify.heroic.async.Future;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.injection.LifeCycle;

/**
 * Handles management of cluster state.
 *
 * The primary responsibility is to receive refresh requests through {@link #refresh()} that should cause the cluster
 * state to be updated.
 *
 * It also provides an interface for looking up nodes through {@link #findNode(Map, NodeCapability)}.
 *
 * @author udoprog
 */
public interface ClusterManager extends LifeCycle {
    @Data
    public static final class Statistics {
        private final int onlineNodes;
        private final int offlineNodes;
    }

    public List<NodeRegistryEntry> getNodes();

    public NodeRegistryEntry findNode(final Map<String, String> tags, NodeCapability capability);

    public Collection<NodeRegistryEntry> findAllShards(NodeCapability capability);

    public Future<Void> refresh();

    public Statistics getStatistics();

    public boolean isAnyV(Collection<NodeRegistryEntry> nodes, int version);
}
