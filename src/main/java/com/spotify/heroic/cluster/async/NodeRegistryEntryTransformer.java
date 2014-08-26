package com.spotify.heroic.cluster.async;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.DiscoveredClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc0.Rpc0ClusterNode;

public class NodeRegistryEntryTransformer implements
        Callback.Transformer<NodeMetadata, NodeRegistryEntry> {
    @Override
    public NodeRegistryEntry transform(NodeMetadata metadata) throws Exception {
        final ClusterNode node = buildClusterNode(metadata);
        return new NodeRegistryEntry(node, metadata);
    }

    private ClusterNode buildClusterNode(NodeMetadata metadata)
            throws Exception {
        final DiscoveredClusterNode discovered = metadata.getDiscovered();

        switch (metadata.getVersion()) {
        case 0:
            return new Rpc0ClusterNode(discovered.getUrl(),
                    discovered.getConfig(), discovered.getExecutor());
        default:
            throw new Exception("Unsupported RPC version '"
                    + metadata.getVersion() + "' for node: " + discovered);
        }
    }
}
