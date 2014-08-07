package com.spotify.heroic.cluster.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.cluster.NodeRegistry;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;

@Slf4j
@RequiredArgsConstructor
public final class NodeRegistryEntryReducer implements
Callback.Reducer<NodeRegistryEntry, Void> {
    private final AtomicReference<NodeRegistry> registry;

    @Override
    public Void resolved(
            Collection<NodeRegistryEntry> results,
            Collection<Exception> errors,
            Collection<CancelReason> cancelled)
                    throws Exception {
        log.info("Updating cluster registry with: {}", results);
        registry.set(new NodeRegistry(new ArrayList<>(results)));
        return null;
    }
}
