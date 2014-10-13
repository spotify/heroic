package com.spotify.heroic.cluster;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Named;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Exposed;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeMetadata;

@RequiredArgsConstructor
public class ClusterManagerModule extends PrivateModule {
    private final static Key<ClusterDiscovery> DISCOVERY_KEY = Key.get(ClusterDiscovery.class);

    public static final Set<NodeCapability> DEFAULT_CAPABILITIES = ImmutableSet.copyOf(Sets.newHashSet(
            NodeCapability.QUERY, NodeCapability.WRITE));

    public static final boolean DEFAULT_USE_LOCAL = false;

    private final Map<String, String> localTags;
    private final Set<NodeCapability> capabilities;
    private final UUID localId;
    private final boolean useLocal;
    private final ClusterDiscoveryModule discovery;

    @JsonCreator
    public static ClusterManagerModule create(@JsonProperty("discovery") ClusterDiscoveryModule discovery,
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("capabilities") Set<NodeCapability> capabilities, @JsonProperty("useLocal") Boolean useLocal,
            @JsonProperty("threadPoolSize") Integer threadPoolSize) {
        if (capabilities == null)
            capabilities = DEFAULT_CAPABILITIES;

        if (useLocal == null)
            useLocal = DEFAULT_USE_LOCAL;

        if (discovery == null)
            discovery = ClusterDiscoveryModule.Null.module();

        final UUID id = UUID.randomUUID();

        return new ClusterManagerModule(tags, capabilities, id, useLocal, discovery);
    }

    public static ClusterManagerModule createDefault() {
        return create(null, null, null, null, null);
    }

    @Provides
    @Exposed
    public NodeMetadata localMetadata() {
        return ClusterRPC.localMetadata(localId, localTags, capabilities);
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
        install(discovery.module(DISCOVERY_KEY));

        bind(ClusterRPC.class).in(Scopes.SINGLETON);

        bind(ClusterManager.class).to(ClusterManagerImpl.class).in(Scopes.SINGLETON);
        expose(ClusterManager.class);
    }
}
