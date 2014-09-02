package com.spotify.heroic.cluster.async;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.DiscoveredClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc0.Rpc0ClusterNode;
import com.spotify.heroic.http.rpc1.Rpc1ClusterNode;
import com.spotify.heroic.http.rpc2.Rpc2ClusterNode;

@RequiredArgsConstructor
public class NodeRegistryEntryTransformer implements
Callback.Transformer<NodeMetadata, NodeRegistryEntry> {
    private final NodeRegistryEntry localEntry;

    @Override
    public NodeRegistryEntry transform(NodeMetadata metadata) throws Exception {
        final ClusterNode node = buildClusterNode(metadata);
        return new NodeRegistryEntry(node, metadata);
    }

    /**
     * Pick the best cluster node implementation depending on the provided
     * metadata.
     *
     * @param metadata
     * @return
     * @throws Exception
     */
    private ClusterNode buildClusterNode(NodeMetadata metadata)
            throws Exception {
        final DiscoveredClusterNode discovered = metadata.getDiscovered();

        if (metadata.getId().equals(
                localEntry.getMetadata().getId())) {
            return localEntry.getClusterNode();
        }

        switch (metadata.getVersion()) {
        case 0:
            return new Rpc0ClusterNode(discovered.getUrl(),
                    discovered.getConfig(), discovered.getExecutor());
        case 1:
            return new Rpc1ClusterNode(discovered.getUrl(),
                    discovered.getConfig(), discovered.getExecutor());
        default:
            return new Rpc2ClusterNode(discovered.getUrl(),
                    discovered.getConfig(), discovered.getExecutor());
        }
    }
}
