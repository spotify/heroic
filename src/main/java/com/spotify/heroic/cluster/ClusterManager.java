package com.spotify.heroic.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cluster.async.NodeRegistryEntryTransformer;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc.RpcResource;
import com.spotify.heroic.injection.LifeCycle;

@Slf4j
@Data
public class ClusterManager implements LifeCycle {
    private final ClusterDiscovery discovery;
    private final Map<String, String> localNodeTags;
    private final Set<NodeCapability> capabilities;
    private final UUID localNodeId;
    private final NodeRegistryEntry localEntry;
    private final LocalClusterNode localClusterNode;

    @Data
    public static final class Statistics {
        private final int onlineNodes;
        private final int offlineNodes;
    }

    @JsonCreator
    public static ClusterManager create(
            @JsonProperty("discovery") ClusterDiscovery discovery,
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("capabilities") Set<NodeCapability> capabilities) {
        if (discovery == null)
            discovery = ClusterDiscovery.NULL;

        final UUID id = UUID.randomUUID();

        final LocalClusterNode localClusterNode = new LocalClusterNode();

        final NodeRegistryEntry localEntry = buildLocalEntry(localClusterNode,
                id, tags, capabilities);
        /*
         * final NodeRegistryEntry localEntry = ClusterManagerImpl
         * .buildLocalEntry(metrics, id, tags, capabilities);
         */

        return new ClusterManager(discovery, tags, capabilities, id,
                localEntry, localClusterNode);
    }

    private final AtomicReference<NodeRegistry> registry = new AtomicReference<>(
            null);

    public NodeRegistryEntry findNode(final Map<String, String> tags,
            NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        return registry.findEntry(tags, capability);
    }

    public Callback<Void> refresh() {
        if (discovery == ClusterDiscovery.NULL) {
            log.info("No discovery mechanism configured");
            registry.set(new NodeRegistry(Arrays
                    .asList(new NodeRegistryEntry[] { localEntry }), 1));
            return new ResolvedCallback<Void>(null);
        }

        log.info("Cluster refresh in progress");

        final Callback.DeferredTransformer<Collection<DiscoveredClusterNode>, Void> transformer = new Callback.DeferredTransformer<Collection<DiscoveredClusterNode>, Void>() {
            @Override
            public Callback<Void> transform(
                    final Collection<DiscoveredClusterNode> nodes)
                            throws Exception {
                final List<Callback<NodeRegistryEntry>> callbacks = new ArrayList<>(
                        nodes.size());

                for (final DiscoveredClusterNode node : nodes) {
                    final NodeRegistryEntryTransformer transformer = new NodeRegistryEntryTransformer(
                            node, localEntry);
                    callbacks.add(node.getMetadata().transform(transformer));
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

                        log.info(String
                                .format("Updated cluster registry with %d nodes (%d failed, %d cancelled)",
                                        results.size(), errors.size(),
                                        cancelled.size()));
                        registry.set(new NodeRegistry(new ArrayList<>(results),
                                nodes.size()));
                        return null;
                    }
                };

                return ConcurrentCallback.newReduce(callbacks, reducer);
            }
        };

        return discovery.getNodes().transform(transformer);
    }

    public ClusterManager.Statistics getStatistics() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            return null;

        return new ClusterManager.Statistics(registry.getOnlineNodes(),
                registry.getOfflineNodes());
    }

    @Override
    public void start() throws Exception {
        log.info("Executing initial refresh");
        refresh().get();
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public boolean isReady() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            return false;

        return registry.getOnlineNodes() > 0;
    }

    public static NodeRegistryEntry buildLocalEntry(
            ClusterNode localClusterNode, UUID localNodeId,
            Map<String, String> localNodeTags, Set<NodeCapability> capabilities) {
        final NodeMetadata metadata = new NodeMetadata(RpcResource.VERSION,
                localNodeId, localNodeTags, capabilities);
        return new NodeRegistryEntry(localClusterNode, metadata);
    }
}
