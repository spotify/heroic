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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.spotify.heroic.common.ServiceInfo;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metadata.MetadataComponent;
import com.spotify.heroic.metric.MetricComponent;
import com.spotify.heroic.suggest.SuggestComponent;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.spotify.heroic.common.Optionals.pickOptional;
import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * @author udoprog
 */
@Data
public class ClusterManagerModule {
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

    public ClusterComponent module(
        PrimaryComponent primary, MetricComponent metric, MetadataComponent metadata,
        SuggestComponent suggest
    ) {
        final ClusterDiscoveryComponent discovery = this.discovery.module(primary);
        return DaggerClusterManagerModule_C
            .builder()
            .primaryComponent(primary)
            .metricComponent(metric)
            .metadataComponent(metadata)
            .suggestComponent(suggest)
            .clusterDiscoveryComponent(discovery)
            .m(new M(primary, metric, metadata, suggest))
            .build();
    }

    @ClusterScope
    @Component(modules = M.class,
        dependencies = {
            PrimaryComponent.class, ClusterDiscoveryComponent.class, MetricComponent.class,
            MetadataComponent.class, SuggestComponent.class
        })
    interface C extends ClusterComponent {
        @Override
        CoreClusterManager clusterManager();

        @Override
        NodeMetadata nodeMetadata();

        @Override
        @Named("cluster")
        LifeCycle clusterLife();
    }

    @RequiredArgsConstructor
    @Module
    class M {
        private final PrimaryComponent primary;
        private final MetricComponent metric;
        private final MetadataComponent metadata;
        private final SuggestComponent suggest;

        @Provides
        @ClusterScope
        public NodeMetadata localMetadata(final ServiceInfo service) {
            return new NodeMetadata(0, id, tags, capabilities, service);
        }

        @Provides
        @ClusterScope
        @Named("useLocal")
        public Boolean useLocal() {
            return useLocal;
        }

        @Provides
        @ClusterScope
        @Named("topology")
        public Set<Map<String, String>> topology() {
            return topology;
        }

        @Provides
        @ClusterScope
        public List<Pair<String, RpcProtocolComponent>> protocolComponents(
            final NodeMetadata nodeMetadata
        ) {
            final ImmutableList.Builder<Pair<String, RpcProtocolComponent>> protocolComponents =
                ImmutableList.builder();

            for (final RpcProtocolModule m : protocols) {
                protocolComponents.add(Pair.of(m.scheme(),
                    m.module(primary, metric, metadata, suggest, nodeMetadata)));
            }

            return protocolComponents.build();
        }

        @Provides
        @ClusterScope
        public Map<String, RpcProtocol> protocols(
            List<Pair<String, RpcProtocolComponent>> protocols
        ) {
            final Map<String, RpcProtocol> map = new HashMap<>();

            for (final Pair<String, RpcProtocolComponent> m : protocols) {
                map.put(m.getLeft(), m.getRight().rpcProtocol());
            }

            return map;
        }

        @Provides
        @ClusterScope
        @Named("cluster")
        LifeCycle clusterLife(
            LifeCycleManager manager, CoreClusterManager cluster,
            List<Pair<String, RpcProtocolComponent>> protocols
        ) {
            final List<LifeCycle> life = new ArrayList<>();
            life.add(manager.build(cluster));
            protocols.stream().map(p -> p.getRight().life()).forEach(life::add);
            return LifeCycle.combined(life);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Optional<UUID> id = empty();
        private Optional<Map<String, String>> tags = empty();
        private Optional<Set<NodeCapability>> capabilities = empty();
        private Optional<Boolean> useLocal = empty();
        private Optional<ClusterDiscoveryModule> discovery = empty();
        private Optional<List<RpcProtocolModule>> protocols = empty();
        private Optional<Set<Map<String, String>>> topology = empty();

        @JsonCreator
        public Builder(
            @JsonProperty("id") Optional<UUID> id,
            @JsonProperty("tags") Optional<Map<String, String>> tags,
            @JsonProperty("capabilities") Optional<Set<NodeCapability>> capabilities,
            @JsonProperty("useLocal") Optional<Boolean> useLocal,
            @JsonProperty("discovery") Optional<ClusterDiscoveryModule> discovery,
            @JsonProperty("protocols") Optional<List<RpcProtocolModule>> protocols,
            @JsonProperty("topology") Optional<Set<Map<String, String>>> topology
        ) {
            this.id = id;
            this.tags = tags;
            this.capabilities = capabilities;
            this.useLocal = useLocal;
            this.discovery = discovery;
            this.protocols = protocols;
            this.topology = topology;
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
