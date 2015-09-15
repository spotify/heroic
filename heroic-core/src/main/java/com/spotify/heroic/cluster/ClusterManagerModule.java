/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.cluster;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Exposed;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.spotify.heroic.HeroicOptions;

import lombok.Data;

/**
 *
 * @author udoprog
 */
@Data
public class ClusterManagerModule {
    private final static Key<ClusterDiscovery> DISCOVERY_KEY = Key.get(ClusterDiscovery.class);
    public static final Set<NodeCapability> DEFAULT_CAPABILITIES = ImmutableSet.copyOf(Sets.newHashSet(
            NodeCapability.QUERY, NodeCapability.WRITE));
    public static final boolean DEFAULT_USE_LOCAL = true;
    public static final Set<Map<String, String>> DEFAULT_TOPOLOGY = ImmutableSet.of();
    public static final List<RpcProtocolModule> DEFAULT_PROTOCOLS = ImmutableList.of();

    private final UUID id;
    private final Map<String, String> tags;
    private final Set<NodeCapability> capabilities;
    private final boolean useLocal;
    private final ClusterDiscoveryModule discovery;
    private final List<RpcProtocolModule> protocols;
    private final Set<Map<String, String>> topology;

    @JsonCreator
    public ClusterManagerModule(@JsonProperty("id") UUID id, @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("topology") Set<Map<String, String>> topology,
            @JsonProperty("capabilities") Set<NodeCapability> capabilities, @JsonProperty("useLocal") Boolean useLocal,
            @JsonProperty("discovery") ClusterDiscoveryModule discovery,
            @JsonProperty("protocols") List<RpcProtocolModule> protocols) {
        this.id = Optional.fromNullable(id).or(defaultId());
        this.tags = tags;
        this.capabilities = Optional.fromNullable(capabilities).or(DEFAULT_CAPABILITIES);
        this.useLocal = Optional.fromNullable(useLocal).or(DEFAULT_USE_LOCAL);
        this.discovery = Optional.fromNullable(discovery).or(ClusterDiscoveryModule.Null.supplier());
        this.protocols = Optional.fromNullable(protocols).or(DEFAULT_PROTOCOLS);
        this.topology = Optional.fromNullable(topology).or(DEFAULT_TOPOLOGY);
    }

    private Supplier<UUID> defaultId() {
        return new Supplier<UUID>() {
            @Override
            public UUID get() {
                return UUID.randomUUID();
            }
        };
    }

    public static Supplier<ClusterManagerModule> defaultSupplier() {
        return new Supplier<ClusterManagerModule>() {
            @Override
            public ClusterManagerModule get() {
                return new ClusterManagerModule(null, null, null, null, null, null, null);
            }
        };
    }

    public Module make(final HeroicOptions options) {
        return new PrivateModule() {
            @Provides
            @Singleton
            @Exposed
            public NodeMetadata localMetadata() {
                return new NodeMetadata(0, id, tags, capabilities);
            }

            @Provides
            @Singleton
            @Named("useLocal")
            public Boolean useLocal() {
                return useLocal;
            }

            @Provides
            @Singleton
            @Named("topology")
            public Set<Map<String, String>> topology() {
                return topology;
            }

            @Override
            protected void configure() {
                install(discovery.module(DISCOVERY_KEY));
                installProtocols(protocols);

                bind(ClusterManager.class).to(CoreClusterManager.class).in(Scopes.SINGLETON);

                expose(ClusterManager.class);
                expose(NodeMetadata.class);
            }

            private void installProtocols(final List<RpcProtocolModule> protocols) {
                final MapBinder<String, RpcProtocol> protocolBindings = MapBinder.newMapBinder(binder(), String.class,
                        RpcProtocol.class);

                for (final RpcProtocolModule m : protocols) {
                    final Key<RpcProtocol> key = Key.get(RpcProtocol.class, Names.named(m.scheme()));
                    install(m.module(key, options));
                    protocolBindings.addBinding(m.scheme()).to(key);
                }
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private UUID id;
        private Map<String, String> tags;
        private Set<NodeCapability> capabilities;
        private Boolean useLocal;
        private ClusterDiscoveryModule discovery;
        private List<RpcProtocolModule> protocols;
        private Set<Map<String, String>> topology;

        public Builder id(UUID id) {
            this.id = id;
            return this;
        }

        public Builder tags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder capabilities(Set<NodeCapability> capabilities) {
            this.capabilities = capabilities;
            return this;
        }

        public Builder useLocal(Boolean useLocal) {
            this.useLocal = useLocal;
            return this;
        }

        public Builder discovery(ClusterDiscoveryModule discovery) {
            this.discovery = discovery;
            return this;
        }

        public Builder protocols(List<RpcProtocolModule> protocols) {
            this.protocols = protocols;
            return this;
        }

        public Builder topology(Set<Map<String, String>> topology) {
            this.topology = topology;
            return this;
        }

        public ClusterManagerModule build() {
            return new ClusterManagerModule(id, tags, topology, capabilities, useLocal, discovery, protocols);
        }
    }
}
