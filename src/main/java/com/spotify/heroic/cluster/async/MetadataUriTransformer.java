package com.spotify.heroic.cluster.async;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.NodeRegistry;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.HttpClientManager;
import com.spotify.heroic.http.HttpClientSession;
import com.spotify.heroic.http.rpc.RpcMetadata;
import com.spotify.heroic.metadata.MetadataManager;

@Data
public final class MetadataUriTransformer implements Callback.DeferredTransformer<Collection<URI>, Void> {
    private final boolean useLocal;

    private final NodeRegistryEntry localEntry;
    private final MetadataManager localMetadata;
    private final HttpClientManager clients;
    private final AtomicReference<NodeRegistry> registry;

    @Override
    public Callback<Void> transform(final Collection<URI> nodes) throws Exception {
        final List<Callback<NodeRegistryEntry>> callbacks = new ArrayList<>(nodes.size());

        for (final URI uri : nodes) {
            callbacks.add(getMetadata(uri).transform(
                    new NodeRegistryEntryTransformer(clients, uri, localEntry, useLocal, localMetadata)));
        }

        return ConcurrentCallback.newReduce(callbacks, new NodeRegistryEntryReducer(registry, nodes));
    }

    public Callback<NodeMetadata> getMetadata(URI uri) {
        final HttpClientSession client = clients.newSession(uri, "rpc");

        final Callback.Transformer<RpcMetadata, NodeMetadata> transformer = new Callback.Transformer<RpcMetadata, NodeMetadata>() {
            @Override
            public NodeMetadata transform(final RpcMetadata r) throws Exception {
                return new NodeMetadata(r.getVersion(), r.getId(), r.getTags(), r.getCapabilities());
            }
        };

        return client.get(RpcMetadata.class, "metadata").transform(transformer);
    }
}