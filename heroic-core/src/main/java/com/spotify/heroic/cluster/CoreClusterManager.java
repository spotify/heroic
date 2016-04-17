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
import com.google.common.collect.Iterables;
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
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

    private final AtomicReference<Set<URI>> staticNodes = new AtomicReference<>(new HashSet<>());

    private final AtomicReference<NodeRegistry> registry = new AtomicReference<>(null);
    private final ConcurrentMap<URI, ClusterNode> clients = new ConcurrentHashMap<>();

    private final Object lock = new Object();

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
        registry.stop(this::stop);
    }

    @Override
    public AsyncFuture<Set<URI>> getStaticNodes() {
        return async.resolved(staticNodes.get());
    }

    @Override
    public AsyncFuture<Void> removeStaticNode(URI node) {
        while (true) {
            final Set<URI> old = staticNodes.get();

            final Set<URI> update = new HashSet<>(staticNodes.get());

            /* node already registered */
            if (!update.remove(node)) {
                return async.resolved();
            }

            if (staticNodes.compareAndSet(old, update)) {
                break;
            }
        }

        return refresh();
    }

    @Override
    public AsyncFuture<Void> addStaticNode(URI node) {
        while (true) {
            final Set<URI> old = staticNodes.get();

            final Set<URI> update = new HashSet<>(staticNodes.get());

            /* node already registered */
            if (!update.add(node)) {
                return async.resolved();
            }

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
        final List<AsyncFuture<List<URI>>> dynamic = new ArrayList<>();

        final List<URI> staticNodes = new ArrayList<>(this.staticNodes.get());

        if (!staticNodes.isEmpty()) {
            dynamic.add(async.resolved(staticNodes));
        }

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
            dynamic.add(discovery.find());
        }

        return async.collect(dynamic, lists -> {
            return ImmutableList.copyOf(Iterables.concat(lists));
        }).lazyTransform(uris -> {
            final List<AsyncFuture<MaybeError<ClusterNode>>> nodes = new ArrayList<>();
            final List<Pair<URI, Supplier<AsyncFuture<Void>>>> removed = new ArrayList<>();

            synchronized (lock) {
                final Set<URI> removedNodes = new HashSet<>(clients.keySet());

                for (final URI uri : uris) {
                    final ClusterNode node = clients.get(uri);
                    removedNodes.remove(uri);

                    if (node != null) {
                        nodes.add(async.resolved(MaybeError.just(node)));
                    } else {
                        log.info("Registering new node {}", uri);

                        /* resolve new node */
                        nodes.add(createClusterNode(uri).onResolved(newNode -> {
                            synchronized (lock) {
                                clients.put(uri, newNode);
                            }
                        }).directTransform(MaybeError::just).catchFailed(MaybeError::error));
                    }
                }

                for (final URI uri : removedNodes) {
                    log.info("Removing lost node {}", uri);

                    final ClusterNode remove = clients.remove(uri);

                    if (remove != null) {
                        removed.add(Pair.of(uri, remove::close));
                    }
                }
            }

            return async.collect(nodes).lazyTransform(newNodes -> {
                final Set<NodeRegistryEntry> entries = new HashSet<>();
                final List<Throwable> failures = new ArrayList<>();

                for (final MaybeError<ClusterNode> maybe : newNodes) {
                    if (maybe.isError()) {
                        failures.add(maybe.getError());
                        continue;
                    }

                    entries.add(createRegistryEntry(maybe.getJust()));
                }

                final Set<Map<String, String>> knownShards = extractKnownShards(entries);

                log.info("Registering known shards: {}", knownShards);
                reporter.registerShards(knownShards);

                log.info("Updated registry: {} result(s), {} failure(s)", entries.size(),
                    failures.size());

                registry.getAndSet(
                    new NodeRegistry(async, new ArrayList<>(entries), entries.size()));

                if (removed.isEmpty()) {
                    return async.resolved();
                }

                log.info("Shutting down {} removed nodes", removed.size());

                /* shutdown removed node */
                return async.collectAndDiscard(removed.stream().map(r -> {
                    final URI uri = r.getLeft();

                    return r.getRight().get().catchFailed(e -> {
                        log.error("Failed to stop node {}", uri, e);
                        return null;
                    });
                }).collect(Collectors.toList()));
            });
        });
    }

    @Override
    public ClusterManager.Statistics getStatistics() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            return null;
        }

        return new ClusterManager.Statistics(registry.getOnlineNodes(), registry.getOfflineNodes());
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

    private AsyncFuture<Void> stop() {
        return async.collectAndDiscard(
            clients.values().stream().map(ClusterNode::close).collect(Collectors.toList()));
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

    private Set<Map<String, String>> extractKnownShards(Set<NodeRegistryEntry> entries) {
        final Set<Map<String, String>> knownShards = new HashSet<>();

        for (final NodeRegistryEntry e : entries) {
            knownShards.add(e.getMetadata().getTags());
        }

        return knownShards;
    }

    private Transform<Throwable, MaybeError<NodeRegistryEntry>> handleError(final URI uri) {
        return error -> {
            log.error("Failed to connect {}", uri, error);
            return MaybeError.error(error);
        };
    }

    /**
     * Resolve the given URI to a specific protocol implementation (then use it, if found).
     *
     * @param uri The uri to resolve.
     * @return A future containing a node registry entry.
     */
    private AsyncFuture<ClusterNode> createClusterNode(final URI uri) {
        final RpcProtocol protocol = protocols.get(uri.getScheme());

        if (protocol == null) {
            throw new IllegalArgumentException("Unsupported scheme (" + uri.getScheme() + ")");
        }

        return protocol.connect(uri);
    }

    private NodeRegistryEntry createRegistryEntry(final ClusterNode node) {
        if (useLocal && local.isPresent() &&
            localMetadata.getId().equals(node.metadata().getId())) {
            log.info("Using local instead of {}", node);

            final LocalClusterNode l = local.get();

            return new NodeRegistryEntry(l, l.metadata());
        }

        return new NodeRegistryEntry(node, node.metadata());
    }
}
