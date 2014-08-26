package com.spotify.heroic.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.async.NodeRegistryEntryTransformer;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
@RequiredArgsConstructor
public class ClusterManagerImpl implements ClusterManager {
    @Data
    public static final class YAML {
        private ClusterDiscovery.YAML discovery;
        private Map<String, String> tags = new HashMap<String, String>();

        public ClusterManagerImpl build(String context)
                throws ValidationException {
            Utils.notNull(context + ".discovery", discovery);
            Utils.notEmpty(context + ".tags", tags);
            final ClusterDiscovery discovery = this.discovery.build(context
                    + ".discovery");
            return new ClusterManagerImpl(discovery, UUID.randomUUID(), tags);
        }
    }

    private final ClusterDiscovery discovery;

    @Getter
    private final UUID localNodeId;
    @Getter
    private final Map<String, String> localNodeTags;

    private final AtomicReference<NodeRegistry> registry = new AtomicReference<>();

    @Override
    public NodeRegistryEntry findNode(final Map<String, String> tags) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        return registry.findEntry(tags);
    }

    @Override
    public Callback<Void> refresh() {
        log.info("Cluster refresh in progress");

        final Callback.DeferredTransformer<Collection<DiscoveredClusterNode>, Void> transformer = new Callback.DeferredTransformer<Collection<DiscoveredClusterNode>, Void>() {
            @Override
            public Callback<Void> transform(
                    Collection<DiscoveredClusterNode> result) throws Exception {
                final List<Callback<NodeRegistryEntry>> callbacks = new ArrayList<>(
                        result.size());

                for (final DiscoveredClusterNode discovered : result) {
                    callbacks.add(discovered.getMetadata().transform(
                            new NodeRegistryEntryTransformer()));
                }

                final Callback.Reducer<NodeRegistryEntry, Void> reducer = new Callback.Reducer<NodeRegistryEntry, Void>() {
                    @Override
                    public Void resolved(Collection<NodeRegistryEntry> results,
                            Collection<Exception> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        for (final Exception error : errors) {
                            log.error("Failed to refresh metadata", error);
                        }

                        for (final CancelReason reason : cancelled) {
                            log.error("Metadata refresh cancelled: " + reason);
                        }

                        log.info("Updated cluster registry with: " + results);
                        registry.set(new NodeRegistry(new ArrayList<>(results),
                                results.size()));
                        return null;
                    }
                };

                return ConcurrentCallback.newReduce(callbacks, reducer);
            }
        };

        return discovery.getNodes().transform(transformer);
    }

    @Override
    public ClusterManager.Statistics getStatistics() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            return null;

        return new ClusterManager.Statistics(registry.getOnlineNodes(),
                registry.getOfflineNodes());
    }
}
