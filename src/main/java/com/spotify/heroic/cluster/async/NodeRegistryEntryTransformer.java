package com.spotify.heroic.cluster.async;

import java.net.URI;
import java.util.concurrent.Executor;

import lombok.RequiredArgsConstructor;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc.RpcNodeException;
import com.spotify.heroic.http.rpc3.Rpc3ClusterNode;
import com.spotify.heroic.http.rpc4.Rpc4ClusterNode;

@RequiredArgsConstructor
public class NodeRegistryEntryTransformer implements
        Callback.Transformer<NodeMetadata, NodeRegistryEntry> {
    private final URI uri;
    private final ClientConfig config;
    private final Executor executor;
    private final NodeRegistryEntry localEntry;
    private final boolean useLocal;

    @Override
    public NodeRegistryEntry transform(NodeMetadata metadata) throws Exception {
        if (useLocal
                && metadata.getId().equals(localEntry.getMetadata().getId())) {
            return localEntry;
        }

        final ClusterNode node = buildClusterNode(metadata);
        return new NodeRegistryEntry(uri, node, metadata);
    }

    /**
     * Pick the best cluster node implementation depending on the provided
     * metadata.
     *
     * @param m
     * @return
     * @throws Exception
     */
    private ClusterNode buildClusterNode(NodeMetadata m) throws Exception {
        final String base = String.format("rpc%d", m.getVersion());

        switch (m.getVersion()) {
        case 0:
        case 1:
        case 2:
            throw new RpcNodeException(uri, "Unsupported RPC version: "
                    + m.getVersion());
        case 3:
            return new Rpc3ClusterNode(base, uri, config, executor);
        default:
            return new Rpc4ClusterNode(base, uri, config, executor);
        }
    }
}
