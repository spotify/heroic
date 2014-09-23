package com.spotify.heroic.cluster.async;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.cluster.NodeRegistry;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc.RpcNodeException;

@Data
@Slf4j
public final class NodeRegistryEntryReducer implements
        Callback.Reducer<NodeRegistryEntry, Void> {
    private final AtomicReference<NodeRegistry> registry;
    private final Collection<URI> nodes;

    @Override
    public Void resolved(Collection<NodeRegistryEntry> results,
            Collection<Exception> errors, Collection<CancelReason> cancelled)
            throws Exception {
        handleErrors(errors);
        handleCancelled(cancelled);

        log.info(String
                .format("Updated cluster registry with %d nodes (%d failed, %d cancelled)",
                        results.size(), errors.size(), cancelled.size()));

        registry.set(new NodeRegistry(new ArrayList<>(results), nodes.size()));
        return null;
    }

    private void handleCancelled(Collection<CancelReason> cancelled) {
        for (final CancelReason reason : cancelled)
            log.error("Metadata refresh cancelled: " + reason);
    }

    private void handleErrors(Collection<Exception> errors) {
        for (final Exception error : errors)
            handleError(error);
    }

    private void handleError(final Exception error) {
        if (error instanceof RpcNodeException) {
            final RpcNodeException e = (RpcNodeException) error;
            final String cause = e.getCause() == null ? "no cause" : e
                    .getCause().getMessage();
            log.error(String.format(
                    "Failed to refresh metadata for %s: %s (%s)", e.getUri(),
                    e.getMessage(), cause));
        } else {
            log.error("Failed to refresh metadata", error);
        }
    }
};
