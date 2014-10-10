package com.spotify.heroic.cluster.async;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;

import com.spotify.heroic.async.DeferredTransformer;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.cluster.ClusterRPC;
import com.spotify.heroic.cluster.NodeRegistry;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;

@Data
public final class MetadataUriTransformer implements DeferredTransformer<Collection<URI>, Void> {
    private final ClusterRPC rpcClients;
    private final AtomicReference<NodeRegistry> registry;

    @Override
    public Future<Void> transform(final Collection<URI> nodes) throws Exception {
        final List<Future<NodeRegistryEntry>> callbacks = new ArrayList<>(nodes.size());

        for (final URI uri : nodes) {
            callbacks.add(rpcClients.resolve(uri));
        }

        return Futures.reduce(callbacks, new NodeRegistryEntryReducer(registry, nodes));
    }
}