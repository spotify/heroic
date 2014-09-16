package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.glassfish.jersey.client.ClientConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cluster.async.NodeRegistryEntryTransformer;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc.RpcGetRequestResolver;
import com.spotify.heroic.http.rpc.RpcMetadata;
import com.spotify.heroic.http.rpc.RpcNodeException;
import com.spotify.heroic.http.rpc.RpcResource;
import com.spotify.heroic.injection.LifeCycle;

/**
 * Handles management of cluster state.
 *
 * The primary responsibility is to receive refresh requests through
 * {@link #refresh()} that should cause the cluster state to be updated.
 *
 * It also provides an interface for looking up nodes through
 * {@link #findNode(Map, NodeCapability)}.
 *
 * @author udoprog
 */
@Slf4j
@Data
public class ClusterManager implements LifeCycle {
    public static final Set<NodeCapability> DEFAULT_CAPABILITIES = ImmutableSet
            .copyOf(Sets.newHashSet(NodeCapability.QUERY, NodeCapability.WRITE));

    public static final boolean DEFAULT_USE_LOCAL = false;
    public static final int DEFAULT_THREAD_POOL_SIZE = 24;

    private final ClusterDiscovery discovery;
    private final Map<String, String> localNodeTags;
    private final Set<NodeCapability> capabilities;
    private final UUID localNodeId;
    private final NodeRegistryEntry localEntry;
    private final LocalClusterNode localClusterNode;
    private final boolean useLocal;
    private final ClientConfig clientConfig;
    private final Executor executor;

    @Data
    public static final class Statistics {
        private final int onlineNodes;
        private final int offlineNodes;
    }

    @JsonCreator
    public static ClusterManager create(
            @JsonProperty("discovery") ClusterDiscovery discovery,
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("capabilities") Set<NodeCapability> capabilities,
            @JsonProperty("useLocal") Boolean useLocal,
            @JsonProperty("threadPoolSize") Integer threadPoolSize) {
        if (discovery == null)
            discovery = ClusterDiscovery.NULL;

        if (capabilities == null)
            capabilities = DEFAULT_CAPABILITIES;

        if (useLocal == null)
            useLocal = DEFAULT_USE_LOCAL;

        if (threadPoolSize == null)
            threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

        final UUID id = UUID.randomUUID();

        final LocalClusterNode localClusterNode = new LocalClusterNode(id);

        final NodeRegistryEntry localEntry = buildLocalEntry(localClusterNode,
                id, tags, capabilities);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);

        final Executor executor = Executors.newFixedThreadPool(threadPoolSize);

        return new ClusterManager(discovery, tags, capabilities, id,
                localEntry, localClusterNode, useLocal, clientConfig, executor);
    }

    private final AtomicReference<NodeRegistry> registry = new AtomicReference<>(
            null);

    public List<NodeRegistryEntry> getNodes() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            throw new IllegalStateException("Registry not ready");

        return registry.getEntries();
    }

    public NodeRegistryEntry findNode(final Map<String, String> tags,
            NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            throw new IllegalStateException("Registry not ready");

        return registry.findEntry(tags, capability);
    }

    public Collection<NodeRegistryEntry> findAllShards(NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            throw new IllegalStateException("Registry not ready");

        return registry.findAllShards(capability);
    }

    public Callback<Void> refresh() {
        if (discovery == ClusterDiscovery.NULL) {
            log.info("No discovery mechanism configured");
            registry.set(new NodeRegistry(Lists.newArrayList(localEntry), 1));
            return new ResolvedCallback<Void>(null);
        }

        log.info("Cluster refresh in progress");

        final Callback.DeferredTransformer<Collection<URI>, Void> transformer = new Callback.DeferredTransformer<Collection<URI>, Void>() {
            @Override
            public Callback<Void> transform(final Collection<URI> nodes)
                    throws Exception {
                final List<Callback<NodeRegistryEntry>> callbacks = new ArrayList<>(
                        nodes.size());

                for (final URI node : nodes) {
                    final NodeRegistryEntryTransformer transformer = new NodeRegistryEntryTransformer(
                            node, clientConfig, executor, localEntry, useLocal);
                    callbacks.add(getMetadata(node).transform(transformer));
                }

                final Callback.Reducer<NodeRegistryEntry, Void> reducer = new Callback.Reducer<NodeRegistryEntry, Void>() {
                    @Override
                    public Void resolved(Collection<NodeRegistryEntry> results,
                            Collection<Exception> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        for (final Exception error : errors) {
                            if (error instanceof RpcNodeException) {
                                final RpcNodeException e = (RpcNodeException) error;
                                final String cause = e.getCause() == null ? "no cause"
                                        : e.getCause().getMessage();
                                log.error(String
                                        .format("Failed to refresh metadata for %s: %s (%s)",
                                                e.getUri(), e.getMessage(),
                                                cause));
                            } else {
                                log.error("Failed to refresh metadata", error);
                            }
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

        return discovery.find().transform(transformer);
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
        return new NodeRegistryEntry(null, localClusterNode, metadata);
    }

    public Callback<NodeMetadata> getMetadata(URI uri) {
        final Client client = ClientBuilder.newClient(clientConfig);
        final WebTarget target = client.target(uri).path("rpc")
                .path("metadata");

        final RpcGetRequestResolver<RpcMetadata> resolver = new RpcGetRequestResolver<RpcMetadata>(
                RpcMetadata.class, target);

        final Callback.Transformer<RpcMetadata, NodeMetadata> transformer = new Callback.Transformer<RpcMetadata, NodeMetadata>() {
            @Override
            public NodeMetadata transform(final RpcMetadata r) throws Exception {
                return new NodeMetadata(r.getVersion(), r.getId(), r.getTags(),
                        r.getCapabilities());
            }
        };

        return ConcurrentCallback.newResolve(executor, resolver).transform(
                transformer);
    }
}
