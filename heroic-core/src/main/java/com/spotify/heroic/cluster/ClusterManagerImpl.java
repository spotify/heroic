package com.spotify.heroic.cluster;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Lists;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.ResolvedFuture;
import com.spotify.heroic.cluster.async.MetadataUriTransformer;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.injection.LifeCycle;

/**
 * Handles management of cluster state.
 *
 * The primary responsibility is to receive refresh requests through {@link #refresh()} that should cause the cluster
 * state to be updated.
 *
 * It also provides an interface for looking up nodes through {@link #findNode(Map, NodeCapability)}.
 *
 * @author udoprog
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@ToString
public class ClusterManagerImpl implements ClusterManager, LifeCycle {
    @Inject
    private ClusterDiscovery discovery;

    @Inject
    private ClusterRPC rpc;

    final AtomicReference<NodeRegistry> registry = new AtomicReference<>(null);

    @Override
    public List<NodeRegistryEntry> getNodes() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            throw new IllegalStateException("Registry not ready");

        return registry.getEntries();
    }

    @Override
    public NodeRegistryEntry findNode(final Map<String, String> tags, NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            throw new IllegalStateException("Registry not ready");

        return registry.findEntry(tags, capability);
    }

    @Override
    public Collection<NodeRegistryEntry> findAllShards(NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            throw new IllegalStateException("Registry not ready");

        return registry.findAllShards(capability);
    }

    @Override
    public Future<Void> refresh() {
        if (discovery == null) {
            log.info("No discovery mechanism configured");
            registry.set(new NodeRegistry(Lists.newArrayList(rpc.localEntry()), 1));
            return new ResolvedFuture<Void>(null);
        }

        log.info("Cluster refresh in progress");

        return discovery.find().transform(new MetadataUriTransformer(rpc, registry));
    }

    @Override
    public ClusterManager.Statistics getStatistics() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            return null;

        return new ClusterManager.Statistics(registry.getOnlineNodes(), registry.getOfflineNodes());
    }

    @Override
    public boolean isAnyV(Collection<NodeRegistryEntry> nodes, int version) {
        for (final NodeRegistryEntry node : nodes) {
            if (node.getMetadata().getVersion() <= version) {
                return true;
            }
        }

        return false;
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
