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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.ServiceInfo;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metadata.MetadataComponent;
import com.spotify.heroic.metric.MetricComponent;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.QueryReporter;
import com.spotify.heroic.suggest.SuggestComponent;
import dagger.Module;
import dagger.Provides;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.inject.Named;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author udoprog
 */
@Data
@Module
public class ClusterManagerModule {
    public static final boolean DEFAULT_USE_LOCAL = true;

    private final UUID id;
    private final Map<String, String> tags;
    private final boolean useLocal;
    private final ClusterDiscoveryModule discovery;
    private final List<RpcProtocolModule> protocols;
    private final Set<Map<String, String>> topology;

    @Provides
    @ClusterScope
    @Named("local")
    public ClusterNode localClusterNode(LocalClusterNode local) {
        return local;
    }

    @Provides
    @ClusterScope
    public NodeMetadata localMetadata(final ServiceInfo service) {
        return new NodeMetadata(0, id, tags, service);
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
    public QueryReporter queryReporter(HeroicReporter heroicReporter) {
        return heroicReporter.newQueryReporter();
    }

    @Provides
    @ClusterScope
    public List<Pair<String, RpcProtocolComponent>> protocolComponents(
        final NodeMetadata nodeMetadata, @Named("local") final ClusterNode localClusterNode,
        final PrimaryComponent primary, final MetricComponent metric,
        final MetadataComponent metadata, final SuggestComponent suggest
    ) {
        final ImmutableList.Builder<Pair<String, RpcProtocolComponent>> protocolComponents =
            ImmutableList.builder();

        /* build up a local component which defines all dependencies for a child component */
        final RpcProtocolModule.Dependencies dependencies = DaggerRpcProtocolModule_Dependencies
            .builder()
            .primaryComponent(primary)
            .metricComponent(metric)
            .metadataComponent(metadata)
            .suggestComponent(suggest)
            .provided(new RpcProtocolModule.Provided() {
                @Override
                public NodeMetadata metadata() {
                    return nodeMetadata;
                }

                @Override
                public ClusterNode localClusterNode() {
                    return localClusterNode;
                }
            })
            .build();

        for (final RpcProtocolModule m : protocols) {
            protocolComponents.add(Pair.of(m.scheme(), m.module(dependencies)));
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

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Optional<UUID> id = empty();
        private Optional<Map<String, String>> tags = empty();
        private Optional<Boolean> useLocal = empty();
        private Optional<ClusterDiscoveryModule> discovery = empty();
        private Optional<List<RpcProtocolModule>> protocols = empty();
        private Optional<Set<Map<String, String>>> topology = empty();

        @JsonCreator
        public Builder(
            @JsonProperty("id") Optional<UUID> id,
            @JsonProperty("tags") Optional<Map<String, String>> tags,
            @JsonProperty("useLocal") Optional<Boolean> useLocal,
            @JsonProperty("discovery") Optional<ClusterDiscoveryModule> discovery,
            @JsonProperty("protocols") Optional<List<RpcProtocolModule>> protocols,
            @JsonProperty("topology") Optional<Set<Map<String, String>>> topology
        ) {
            this.id = id;
            this.tags = tags;
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
                useLocal.orElse(DEFAULT_USE_LOCAL),
                discovery.orElseGet(ClusterDiscoveryModule::nullModule),
                protocols.orElseGet(ImmutableList::of),
                topology.orElseGet(ImmutableSet::of)
            );
            // @formatter:on
        }
    }
}
