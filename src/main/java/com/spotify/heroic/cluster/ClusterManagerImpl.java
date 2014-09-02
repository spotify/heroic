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

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cluster.async.NodeRegistryEntryTransformer;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc.RpcResource;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
@Data
public class ClusterManagerImpl implements ClusterManager, LifeCycle {
    @Data
    public static final class YAML {
        private ClusterDiscovery.YAML discovery = null;
        private Map<String, String> tags = null;
        private Set<NodeCapability> capabilities = null;

        public ClusterManagerImpl build(ConfigContext context,
                MetricBackendManager metrics) throws ValidationException {
            final ClusterDiscovery discovery;

            if (this.discovery == null) {
                discovery = ClusterDiscovery.NULL;
            } else {
                discovery = this.discovery.build(context.extend("discovery"));
            }

            return new ClusterManagerImpl(discovery, metrics,
                    UUID.randomUUID(), tags, capabilities);
        }
    }

    private final ClusterDiscovery discovery;
    private final MetricBackendManager metrics;
    private final UUID localNodeId;
    private final Map<String, String> localNodeTags;
    private final Set<NodeCapability> capabilities;
    private final NodeRegistryEntry localEntry;

    public ClusterManagerImpl(ClusterDiscovery discovery,
            MetricBackendManager metrics, UUID localNodeId,
            Map<String, String> localNodeTags, Set<NodeCapability> capabilities) {
        this.discovery = discovery;
        this.metrics = metrics;
        this.localNodeId = localNodeId;
        this.localNodeTags = localNodeTags;
        this.capabilities = capabilities;
        this.localEntry = buildLocalEntry(localNodeId, localNodeTags,
                capabilities);
    }

    private NodeRegistryEntry buildLocalEntry(UUID localNodeId,
            Map<String, String> localNodeTags, Set<NodeCapability> capabilities) {
        final LocalClusterNode localClusterNode = new LocalClusterNode(metrics);
        final NodeMetadata metadata = new NodeMetadata(null,
                RpcResource.VERSION, localNodeId, localNodeTags, capabilities);
        return new NodeRegistryEntry(localClusterNode, metadata);

    }

    private final AtomicReference<NodeRegistry> registry = new AtomicReference<>(
            null);

    @Override
    public NodeRegistryEntry findNode(final Map<String, String> tags,
            NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        return registry.findEntry(tags, capability);
    }

    @Override
    public Callback<Void> refresh() {
        if (discovery == ClusterDiscovery.NULL) {
            log.info("Cluster refresh in progress, but no discovery mechanism configured");
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
                    callbacks.add(node.getMetadata().transform(
                            new NodeRegistryEntryTransformer(localEntry)));
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

    @Override
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
}
