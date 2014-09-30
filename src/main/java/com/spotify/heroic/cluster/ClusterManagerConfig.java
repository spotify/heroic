package com.spotify.heroic.cluster;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Named;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.rpc.RpcResource;

@Data
public class ClusterManagerConfig {
    public static final Set<NodeCapability> DEFAULT_CAPABILITIES = ImmutableSet
            .copyOf(Sets.newHashSet(NodeCapability.QUERY, NodeCapability.WRITE));

    public static final boolean DEFAULT_USE_LOCAL = false;

    private final Map<String, String> localTags;
    private final Set<NodeCapability> capabilities;
    private final UUID localId;
    private final boolean useLocal;
    private final ClusterDiscoveryConfig discovery;

    @JsonCreator
    public static ClusterManagerConfig create(
            @JsonProperty("discovery") ClusterDiscoveryConfig discovery,
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("capabilities") Set<NodeCapability> capabilities,
            @JsonProperty("useLocal") Boolean useLocal,
            @JsonProperty("threadPoolSize") Integer threadPoolSize) {
        if (capabilities == null)
            capabilities = DEFAULT_CAPABILITIES;

        if (useLocal == null)
            useLocal = DEFAULT_USE_LOCAL;

        final UUID id = UUID.randomUUID();

        return new ClusterManagerConfig(tags, capabilities, id, useLocal,
                discovery);
    }

    public static NodeRegistryEntry buildLocalEntry(
            ClusterNode localClusterNode, UUID localNodeId,
            Map<String, String> localNodeTags, Set<NodeCapability> capabilities) {
        final NodeMetadata metadata = new NodeMetadata(RpcResource.VERSION,
                localNodeId, localNodeTags, capabilities);
        return new NodeRegistryEntry(null, localClusterNode, metadata);
    }

    public Module module() {
        final Key<ClusterDiscovery> discoveryKey = Key
                .get(ClusterDiscovery.class);

        return new PrivateModule() {
            @Provides
            @Named("localEntry")
            public NodeRegistryEntry localEntry(
                    LocalClusterNode localClusterNode) {
                final NodeMetadata metadata = new NodeMetadata(
                        RpcResource.VERSION, localId, localTags, capabilities);
                return new NodeRegistryEntry(null, localClusterNode, metadata);
            }

            @Provides
            @Named("useLocal")
            public Boolean useLocal() {
                return useLocal;
            }

            @Provides
            @Named("localId")
            public UUID localId() {
                return localId;
            }

            @Override
            protected void configure() {
                bind(LocalClusterNode.class).in(Scopes.SINGLETON);
                bind(ClusterManager.class).in(Scopes.SINGLETON);
                install(discovery.module(discoveryKey));
                expose(ClusterManager.class);
            }
        };
    }
}
