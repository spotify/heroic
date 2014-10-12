package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.ArrayList;
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
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.DelayedTransform;
import com.spotify.heroic.async.ErrorReducer;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc.RpcNodeException;
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
            return Futures.resolved(null);
        }

        log.info("Cluster refresh in progress");
        return discovery.find().transform(updateRegistry());
    }

    private DelayedTransform<Collection<URI>, Void> updateRegistry() {
        return new DelayedTransform<Collection<URI>, Void>() {
            @Override
            public Future<Void> transform(final Collection<URI> nodes) throws Exception {
                final List<Future<NodeRegistryEntry>> callbacks = new ArrayList<>(nodes.size());

                for (final URI uri : nodes)
                    callbacks.add(rpc.resolve(uri));

                return Futures.reduce(callbacks, addEntriesToRegistry(nodes.size()));
            }
        };
    }

    private ErrorReducer<NodeRegistryEntry, Void> addEntriesToRegistry(final int size) {
        return new ErrorReducer<NodeRegistryEntry, Void>() {
            @Override
            public Void reduce(Collection<NodeRegistryEntry> results, Collection<CancelReason> cancelled,
                    Collection<Exception> errors) throws Exception {
                for (final Exception error : errors)
                    handleError(error);

                for (final CancelReason reason : cancelled)
                    log.error("Metadata refresh cancelled: " + reason);

                log.info(String.format("Updated cluster registry with %d nodes (%d failed, %d cancelled)",
                        results.size(), errors.size(), cancelled.size()));

                registry.set(new NodeRegistry(new ArrayList<>(results), size));
                return null;
            }

            private void handleError(final Exception error) {
                if (error instanceof RpcNodeException) {
                    final RpcNodeException e = (RpcNodeException) error;
                    final String cause = e.getCause() == null ? "no cause" : e.getCause().getMessage();
                    log.error(String.format("Failed to refresh metadata for %s: %s (%s)", e.getUri(), e.getMessage(),
                            cause));
                } else {
                    log.error("Failed to refresh metadata", error);
                }
            }
        };
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
