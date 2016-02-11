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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.HeroicConfiguration;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.async.MaybeError;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.scheduler.Task;
import com.spotify.heroic.statistics.HeroicReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Handles management of cluster state.
 * <p>
 * The primary responsibility is to receive refresh requests through {@link #refresh()} that should
 * cause the cluster state to be updated.
 *
 * @author udoprog
 */
@ClusterScope
@Slf4j
@ToString(of = {"useLocal"})
public class CoreClusterManager implements ClusterManager, LifeCycles {
    private final AsyncFramework async;
    private final ClusterDiscovery discovery;
    private final NodeMetadata localMetadata;
    private final Map<String, RpcProtocol> protocols;
    private final Scheduler scheduler;
    private final Boolean useLocal;
    private final Set<Map<String, String>> topology;
    private final HeroicReporter reporter;
    private final HeroicConfiguration options;
    private final Optional<LocalClusterNode> local;
    private final HeroicContext context;

    private final ResolvableFuture<Void> initialized;

    private final AtomicReference<Set<URI>> staticNodes =
        new AtomicReference<Set<URI>>(new HashSet<URI>());

    final AtomicReference<NodeRegistry> registry = new AtomicReference<>(null);

    @Inject
    public CoreClusterManager(
        AsyncFramework async, ClusterDiscovery discovery, NodeMetadata localMetadata,
        Map<String, RpcProtocol> protocols, Scheduler scheduler,
        @Named("useLocal") Boolean useLocal, @Named("topology") Set<Map<String, String>> topology,
        HeroicReporter reporter, HeroicConfiguration options, LocalClusterNode local,
        HeroicContext context
    ) {
        this.async = async;
        this.discovery = discovery;
        this.localMetadata = localMetadata;
        this.protocols = protocols;
        this.scheduler = scheduler;
        this.useLocal = useLocal;
        this.topology = topology;
        this.reporter = reporter;
        this.options = options;
        this.local = Optional.fromNullable(local);
        this.context = context;

        this.initialized = async.future();
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
    }

    @Override
    public AsyncFuture<Void> addStaticNode(URI node) {
        while (true) {
            final Set<URI> old = staticNodes.get();

            final Set<URI> update = new HashSet<>(staticNodes.get());
            update.add(node);

            if (staticNodes.compareAndSet(old, update)) {
                break;
            }
        }

        return refresh();
    }

    @Override
    public List<NodeRegistryEntry> getNodes() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        return registry.getEntries();
    }

    @Override
    public AsyncFuture<Void> refresh() {
        final AsyncFuture<List<MaybeError<NodeRegistryEntry>>> transform;

        if (discovery instanceof ClusterDiscoveryModule.Null) {
            final List<MaybeError<NodeRegistryEntry>> results = new ArrayList<>();

            if (useLocal && local.isPresent()) {
                log.info("No discovery mechanism configured, using local node");
                final LocalClusterNode l = local.get();
                results.add(MaybeError.just(new NodeRegistryEntry(l, l.metadata())));
            } else {
                log.warn("No discovery mechanism configured, clustered operations will not work" +
                    " (useLocal: {}, local: {})", useLocal, local);
            }

            transform = async.resolved(results);
        } else {
            log.info("Refreshing cluster");
            transform = discovery.find().lazyTransform(resolve());
        }

        return addStaticNodes(transform).lazyTransform(updateRegistry());
    }

    @Override
    public ClusterManager.Statistics getStatistics() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            return null;
        }

        return new ClusterManager.Statistics(registry.getOnlineNodes(), registry.getOfflineNodes());
    }

    private AsyncFuture<Void> start() {
        final AsyncFuture<Void> startup;

        if (!options.isOneshot()) {
            startup = context.startedFuture().directTransform(result -> {
                scheduler.periodically("cluster-refresh", 1, TimeUnit.MINUTES, new Task() {
                    @Override
                    public void run() throws Exception {
                        refresh().get();
                    }
                });

                return null;
            });
        } else {
            startup = context.startedFuture();
        }

        startup.lazyTransform(result -> refresh().catchFailed((Throwable e) -> {
            log.error("initial metadata refresh failed", e);
            return null;
        })).onFinished(() -> initialized.resolve(null));

        return async.resolved();
    }

    @Override
    public boolean isReady() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            return false;
        }

        return registry.getOnlineNodes() > 0;
    }

    @Override
    public AsyncFuture<Void> initialized() {
        return initialized;
    }

    @Override
    public ClusterNodeGroup useDefaultGroup() {
        return useGroup(null);
    }

    @Override
    public ClusterNodeGroup useGroup(String group) {
        final List<ClusterNode.Group> groups = new ArrayList<>();

        for (final NodeRegistryEntry e : findAllShards(null)) {
            groups.add(e.getClusterNode().useGroup(group));
        }

        return new CoreClusterNodeGroup(async, groups);
    }

    @Override
    public Set<RpcProtocol> protocols() {
        return ImmutableSet.copyOf(protocols.values());
    }

    private List<Map<String, String>> topologyOf(Collection<NodeRegistryEntry> entries) {
        final List<Map<String, String>> shards = new ArrayList<>();

        for (final NodeRegistryEntry e : entries) {
            shards.add(e.getMetadata().getTags());
        }

        return shards;
    }

    private Collection<NodeRegistryEntry> findAllShards(NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        final Collection<NodeRegistryEntry> all = registry.findAllShards(capability);

        if (!topology.isEmpty()) {
            int found = 0;

            for (final NodeRegistryEntry entry : all) {
                if (topology.contains(entry.getMetadata().getTags())) {
                    found += 1;
                }
            }

            if (found != topology.size()) {
                throw new IllegalStateException(
                    String.format("Could not find %s nodes for the whole topology (%s), found (%s)",
                        capability, StringUtils.join(topology, ", "),
                        StringUtils.join(topologyOf(all), ", ")));
            }
        }

        return all;
    }

    // @formatter:off
    static Collector<List<MaybeError<NodeRegistryEntry>>, List<MaybeError<NodeRegistryEntry>>>
        joinMaybeErrors =
                results -> {
                    final List<MaybeError<NodeRegistryEntry>> result = new ArrayList<>();

                    for (final List<MaybeError<NodeRegistryEntry>> r : results) {
                        result.addAll(r);
                    }

                    return result;
                };
    // @formatter:on

    private AsyncFuture<List<MaybeError<NodeRegistryEntry>>> addStaticNodes(
        final AsyncFuture<List<MaybeError<NodeRegistryEntry>>> transform
    ) {
        final List<URI> staticNodes = new ArrayList<>(this.staticNodes.get());

        final AsyncFuture<List<MaybeError<NodeRegistryEntry>>> finalTransform;

        if (!staticNodes.isEmpty()) {
            final List<AsyncFuture<List<MaybeError<NodeRegistryEntry>>>> transforms =
                ImmutableList.of(transform, async.resolved(staticNodes).lazyTransform(resolve()));
            finalTransform = async.collect(transforms, joinMaybeErrors);
        } else {
            finalTransform = transform;
        }

        return finalTransform;
    }

    // @formatter:off
    private static Collector<MaybeError<NodeRegistryEntry>, List<MaybeError<NodeRegistryEntry>>>
            joinMaybeErrorsCollection =
            new Collector<MaybeError<NodeRegistryEntry>, List<MaybeError<NodeRegistryEntry>>>() {
                @Override
                public List<MaybeError<NodeRegistryEntry>> collect(
                        Collection<MaybeError<NodeRegistryEntry>> results) throws Exception {
                    return new ArrayList<>(results);
                }
            };
    // @formatter:on

    private LazyTransform<List<URI>, List<MaybeError<NodeRegistryEntry>>> resolve() {
        return new LazyTransform<List<URI>, List<MaybeError<NodeRegistryEntry>>>() {
            @Override
            public AsyncFuture<List<MaybeError<NodeRegistryEntry>>> transform(final List<URI> nodes)
                throws Exception {
                final List<AsyncFuture<MaybeError<NodeRegistryEntry>>> callbacks =
                    new ArrayList<>(nodes.size());

                for (final URI uri : nodes) {
                    callbacks.add(resolve(uri)
                        .directTransform(MaybeError.transformJust())
                        .catchFailed(handleError(uri)));
                }

                return async.collect(callbacks, joinMaybeErrorsCollection);
            }
        };
    }

    private LazyTransform<List<MaybeError<NodeRegistryEntry>>, Void> updateRegistry() {
        return new LazyTransform<List<MaybeError<NodeRegistryEntry>>, Void>() {
            @Override
            public AsyncFuture<Void> transform(List<MaybeError<NodeRegistryEntry>> results)
                throws Exception {
                final Set<NodeRegistryEntry> entries = new HashSet<>();
                final List<Throwable> failures = new ArrayList<>();

                for (final MaybeError<NodeRegistryEntry> maybe : results) {
                    if (maybe.isError()) {
                        failures.add(maybe.getError());
                        continue;
                    }

                    entries.add(maybe.getJust());
                }

                final Set<Map<String, String>> knownShards = extractKnownShards(entries);

                log.info("Registering known shards: {}", knownShards);
                reporter.registerShards(knownShards);

                log.info("Updated registry: {} result(s), {} failure(s)", entries.size(),
                    failures.size());
                final NodeRegistry oldRegistry = registry.getAndSet(
                    new NodeRegistry(async, new ArrayList<>(entries), results.size()));

                if (oldRegistry == null) {
                    return async.resolved(null);
                }

                log.info("Closing old registry");
                return oldRegistry.close();
            }
        };
    }

    private Set<Map<String, String>> extractKnownShards(Set<NodeRegistryEntry> entries) {
        final Set<Map<String, String>> knownShards = new HashSet<>();

        for (final NodeRegistryEntry e : entries) {
            knownShards.add(e.getMetadata().getTags());
        }

        return knownShards;
    }

    private Transform<Throwable, MaybeError<NodeRegistryEntry>> handleError(final URI uri) {
        return new Transform<Throwable, MaybeError<NodeRegistryEntry>>() {
            @Override
            public MaybeError<NodeRegistryEntry> transform(Throwable error) throws Exception {
                log.error("Failed to connect {}", uri, error);
                return MaybeError.error(error);
            }
        };
    }

    private final LazyTransform<ClusterNode, NodeRegistryEntry> localTransform =
        new LazyTransform<ClusterNode, NodeRegistryEntry>() {
            @Override
            public AsyncFuture<NodeRegistryEntry> transform(final ClusterNode node)
                throws Exception {
                if (useLocal && local.isPresent() &&
                    localMetadata.getId().equals(node.metadata().getId())) {
                    log.info("Using local instead of {}", node);

                    final LocalClusterNode l = local.get();

                    return node
                        .close()
                        .directTransform(r -> new NodeRegistryEntry(l, l.metadata()));
                }

                return async.resolved(new NodeRegistryEntry(node, node.metadata()));
            }
        };

    /**
     * Resolve the given URI to a specific protocol implementation (then use it, if found).
     *
     * @param uri The uri to resolve.
     * @return A future containing a node registry entry.
     */
    private AsyncFuture<NodeRegistryEntry> resolve(final URI uri) {
        final RpcProtocol protocol = protocols.get(uri.getScheme());

        if (protocol == null) {
            throw new IllegalArgumentException("Unsupported scheme '" + uri.getScheme());
        }

        return protocol.connect(uri).lazyTransform(localTransform);
    }
}
