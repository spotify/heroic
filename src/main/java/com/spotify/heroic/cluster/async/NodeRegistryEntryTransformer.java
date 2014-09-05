package com.spotify.heroic.cluster.async;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.DiscoveredClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc1.Rpc1ClusterNode;
import com.spotify.heroic.http.rpc2.Rpc2ClusterNode;

@RequiredArgsConstructor
public class NodeRegistryEntryTransformer implements
        Callback.Transformer<NodeMetadata, NodeRegistryEntry> {
    private final DiscoveredClusterNode discovered;
    private final NodeRegistryEntry localEntry;

    @Override
    public NodeRegistryEntry transform(NodeMetadata metadata) throws Exception {
        if (metadata.getId().equals(localEntry.getMetadata().getId())) {
            return localEntry;
        }

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
        switch (metadata.getVersion()) {
        case 0:
            throw new Exception("Unsupported RPC version: "
                    + metadata.getVersion());
        case 1:
            return new Rpc1ClusterNode(discovered.getUrl(),
                    discovered.getConfig(), discovered.getExecutor());
        default:
            return new Rpc2ClusterNode(discovered.getUrl(),
                    discovered.getConfig(), discovered.getExecutor());
        }
    }
}
