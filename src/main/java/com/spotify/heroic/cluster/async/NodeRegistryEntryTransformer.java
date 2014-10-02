package com.spotify.heroic.cluster.async;

import java.net.URI;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.HttpClientManager;
import com.spotify.heroic.http.HttpClientSession;
import com.spotify.heroic.http.rpc.RpcNodeException;
import com.spotify.heroic.http.rpc4.Rpc4ClusterNode;
import com.spotify.heroic.http.rpc5.Rpc5ClusterNode;
import com.spotify.heroic.metadata.MetadataBackendManager;

@RequiredArgsConstructor
public class NodeRegistryEntryTransformer implements Callback.Transformer<NodeMetadata, NodeRegistryEntry> {
    private final HttpClientManager clients;
    private final URI uri;
    private final NodeRegistryEntry localEntry;
    private final boolean useLocal;
    private final MetadataBackendManager localMetadata;

    @Override
    public NodeRegistryEntry transform(NodeMetadata metadata) throws Exception {
        if (useLocal && metadata.getId().equals(localEntry.getMetadata().getId())) {
            return localEntry;
        }

        final ClusterNode node = buildClusterNode(metadata);
        return new NodeRegistryEntry(uri, node, metadata);
    }

    /**
     * Pick the best cluster node implementation depending on the provided metadata.
     *
     * @param m
     * @return
     * @throws Exception
     */
    private ClusterNode buildClusterNode(NodeMetadata m) throws Exception {
        final String base = String.format("rpc%d", m.getVersion());

        final HttpClientSession client = clients.newSession(uri, base);

        if (m.getVersion() < 4) {
            throw new RpcNodeException(uri, "Unsupported RPC version: " + m.getVersion());
        }

        switch (m.getVersion()) {
        case 4:
            // backwards compatibility entails providing this with access to
            // local metadata.
            return new Rpc4ClusterNode(client, localMetadata);
        default:
            return new Rpc5ClusterNode(client);
        }
    }
}
