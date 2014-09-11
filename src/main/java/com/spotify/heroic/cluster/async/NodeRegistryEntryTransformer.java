package com.spotify.heroic.cluster.async;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.DiscoveredClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc.RpcNodeException;
import com.spotify.heroic.http.rpc3.Rpc3ClusterNode;

@RequiredArgsConstructor
public class NodeRegistryEntryTransformer implements
        Callback.Transformer<NodeMetadata, NodeRegistryEntry> {
    private final DiscoveredClusterNode discovered;
    private final NodeRegistryEntry localEntry;
    private final boolean useLocal;

    @Override
    public NodeRegistryEntry transform(NodeMetadata metadata) throws Exception {
        if (useLocal
                && metadata.getId().equals(localEntry.getMetadata().getId())) {
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
        case 1:
        case 2:
            throw new RpcNodeException(discovered.getUrl(),
                    "Unsupported RPC version: " + metadata.getVersion());
        default:
            return new Rpc3ClusterNode(metadata.getId(), discovered.getUrl(),
                    discovered.getConfig(), discovered.getExecutor());
        }
    }
}
