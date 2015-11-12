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

import static com.spotify.heroic.common.Optionals.pickOptional;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.spotify.heroic.HeroicConfiguration;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 * @author udoprog
 */
@Data
public class ClusterManagerModule {
    private static final Key<ClusterDiscovery> DISCOVERY_KEY = Key.get(ClusterDiscovery.class);
    public static final Set<NodeCapability> DEFAULT_CAPABILITIES =
            ImmutableSet.copyOf(Sets.newHashSet(NodeCapability.QUERY, NodeCapability.WRITE));
    public static final boolean DEFAULT_USE_LOCAL = true;

    private final UUID id;
    private final Map<String, String> tags;
    private final Set<NodeCapability> capabilities;
    private final boolean useLocal;
    private final ClusterDiscoveryModule discovery;
    private final List<RpcProtocolModule> protocols;
    private final Set<Map<String, String>> topology;

    public Module make(final HeroicConfiguration options) {
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
                final MapBinder<String, RpcProtocol> protocolBindings =
                        MapBinder.newMapBinder(binder(), String.class, RpcProtocol.class);

                for (final RpcProtocolModule m : protocols) {
                    final Key<RpcProtocol> key =
                            Key.get(RpcProtocol.class, Names.named(m.scheme()));
                    install(m.module(key, options));
                    protocolBindings.addBinding(m.scheme()).to(key);
                }
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Optional<UUID> id = empty();
        private Optional<Map<String, String>> tags = empty();
        private Optional<Set<NodeCapability>> capabilities = empty();
        private Optional<Boolean> useLocal = empty();
        private Optional<ClusterDiscoveryModule> discovery = empty();
        private Optional<List<RpcProtocolModule>> protocols = empty();
        private Optional<Set<Map<String, String>>> topology = empty();

        @JsonCreator
        public Builder(@JsonProperty("id") UUID id, @JsonProperty("tags") Map<String, String> tags,
                @JsonProperty("topology") Set<Map<String, String>> topology,
                @JsonProperty("capabilities") Set<NodeCapability> capabilities,
                @JsonProperty("useLocal") Boolean useLocal,
                @JsonProperty("discovery") ClusterDiscoveryModule discovery,
                @JsonProperty("protocols") List<RpcProtocolModule> protocols) {
            this.id = ofNullable(id);
            this.tags = ofNullable(tags);
            this.capabilities = ofNullable(capabilities);
            this.useLocal = ofNullable(useLocal);
            this.discovery = ofNullable(discovery);
            this.protocols = ofNullable(protocols);
            this.topology = ofNullable(topology);
        }

        public Builder id(UUID id) {
            this.id = of(id);
            return this;
        }

        public Builder tags(Map<String, String> tags) {
            this.tags = of(tags);
            return this;
        }

        public Builder capabilities(Set<NodeCapability> capabilities) {
            this.capabilities = of(capabilities);
            return this;
        }

        public Builder useLocal(Boolean useLocal) {
            this.useLocal = of(useLocal);
            return this;
        }

        public Builder discovery(ClusterDiscoveryModule discovery) {
            this.discovery = of(discovery);
            return this;
        }

        public Builder protocols(List<RpcProtocolModule> protocols) {
            this.protocols = of(protocols);
            return this;
        }

        public Builder topology(Set<Map<String, String>> topology) {
            this.topology = of(topology);
            return this;
        }

        public Builder merge(Builder o) {
            // @formatter:off
            return new Builder(
                pickOptional(id, o.id),
                pickOptional(tags, o.tags),
                pickOptional(capabilities, o.capabilities),
                pickOptional(useLocal, o.useLocal),
                pickOptional(discovery, o.discovery),
                pickOptional(protocols, o.protocols),
                pickOptional(topology, o.topology)
            );
            // @formatter:on
        }

        public ClusterManagerModule build() {
            // @formatter:off
            return new ClusterManagerModule(
                id.orElseGet(UUID::randomUUID),
                tags.orElseGet(ImmutableMap::of),
                capabilities.orElse(DEFAULT_CAPABILITIES),
                useLocal.orElse(DEFAULT_USE_LOCAL),
                discovery.orElseGet(ClusterDiscoveryModule::nullModule),
                protocols.orElseGet(ImmutableList::of),
                topology.orElseGet(ImmutableSet::of)
            );
            // @formatter:on
        }
    }
}
