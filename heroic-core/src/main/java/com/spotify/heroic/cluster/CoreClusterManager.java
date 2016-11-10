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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.spotify.heroic.HeroicConfiguration;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.scheduler.Scheduler;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
    private final HeroicConfiguration options;
    private final LocalClusterNode local;
    private final HeroicContext context;

    final AtomicReference<Set<URI>> staticNodes = new AtomicReference<>(new HashSet<>());
    final AtomicReference<NodeRegistry> registry = new AtomicReference<>();
    final AtomicReference<Map<URI, ClusterNode>> clients =
        new AtomicReference<>(Collections.emptyMap());
    final AtomicLong refreshId = new AtomicLong();

    @Inject
    public CoreClusterManager(
        AsyncFramework async, ClusterDiscovery discovery, NodeMetadata localMetadata,
        Map<String, RpcProtocol> protocols, Scheduler scheduler,
        @Named("useLocal") Boolean useLocal, HeroicConfiguration options, LocalClusterNode local,
        HeroicContext context
    ) {
        this.async = async;
        this.discovery = discovery;
        this.localMetadata = localMetadata;
        this.protocols = protocols;
        this.scheduler = scheduler;
        this.useLocal = useLocal;
        this.options = options;
        this.local = local;
        this.context = context;
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
    public List<ClusterNode> getNodes() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        return registry.getEntries();
    }

    /**
     * Perform a refresh of the cluster information.
     * <p>
     * A refresh happens in four steps.
     * <ol>
     * <li>discovery</li>
     * <li>sweep</li>
     * <li>log and prepare</li>
     * <li>finalize</li>
     * </ol>
     * </p>
     * <p>
     * The discovery phase adds a collection of URIs provided statically (by {@link #staticNodes}
     * and dynamically (by {@link #discovery}) to be fed into the sweep step.
     * </p>
     * <p>
     * The sweep step takes the existing {@link #clients} map and compares it to the updated list of
     * URIs.
     * </p>
     * <p>
     * The log and prepare step logs information about which operations happened, closes any clients
     * that should be closed and sets up for the final step.
     * </p>
     * <p>
     * The finalize step takes the collection of new clients and node entries, replaces it
     * atomically with the old collection. Finally, if there is a race another refresh operation
     * will be issued.
     * </p>
     *
     * @return a future indicating the state of the refresh.
     */
    @Override
    public AsyncFuture<Void> refresh() {
        final String id = String.format("%08x", refreshId.getAndIncrement());
        log.info("new refresh with id ({})", id);
        return refreshDiscovery(id);
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
    public List<ClusterShard> useOptionalGroup(final Optional<String> group) {
        final ImmutableList.Builder<ClusterShard> shards = ImmutableList.builder();

        for (final Pair<Map<String, String>, List<ClusterNode>> e : findFromAllShards()) {
            shards.add(new ClusterShard(async, e.getKey(), ImmutableList.copyOf(
                e.getValue().stream().map(c -> c.useOptionalGroup(group)).iterator())));
        }

        return shards.build();
    }

    @Override
    public Set<RpcProtocol> protocols() {
        return ImmutableSet.copyOf(protocols.values());
    }

    AsyncFuture<Void> start() {
        final AsyncFuture<Void> startup;

        if (!options.isOneshot()) {
            startup = context.startedFuture().directTransform(result -> {
                scheduler.periodically("cluster-refresh", 1, TimeUnit.MINUTES,
                    () -> refresh().get());

                return null;
            });
        } else {
            startup = context.startedFuture();
        }

        startup.lazyTransform(result -> refresh().catchFailed((Throwable e) -> {
            log.error("initial metadata refresh failed", e);
            return null;
        }));

        return async.resolved();
    }

    AsyncFuture<Void> stop() {
        final Map<URI, ClusterNode> clients = this.clients.getAndSet(null);

        if (clients == null) {
            return async.resolved();
        }

        return async.collectAndDiscard(
            clients.values().stream().map(ClusterNode::close).collect(Collectors.toList()));
    }

    List<Pair<Map<String, String>, List<ClusterNode>>> findFromAllShards() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        return registry.findFromAllShards(OptionalLimit.empty());
    }

    Set<Map<String, String>> extractKnownShards(Set<ClusterNode> entries) {
        final Set<Map<String, String>> knownShards = new HashSet<>();

        for (final ClusterNode e : entries) {
            knownShards.add(e.metadata().getTags());
        }

        return knownShards;
    }

    AsyncFuture<Update> createClusterNode(final String id, final URI uri) {
        final RpcProtocol protocol = protocols.get(uri.getScheme());

        if (protocol == null) {
            return async.resolved(new FailedUpdate(uri,
                new IllegalArgumentException("Unsupported protocol (" + uri.getScheme() + ")")));
        }

        return protocol.connect(uri).<Update>lazyTransform(node -> {
            if (useLocal && localMetadata.getId().equals(node.metadata().getId())) {
                log.info("{} using local instead of {} (closing old node)", id, node);
                // close old node
                return node.close().directTransform(v -> new SuccessfulUpdate(uri, true, local));
            }

            return async.resolved(new SuccessfulUpdate(uri, true, node));
        }).catchFailed(Update.error(uri));
    }

    /**
     * The first step of the refresh operation.
     * <p>
     * Discover a new collection of heroic peers, and feed them into the sweep step.
     *
     * @param id id of the operation
     * @return a future indicating when the operation is finished
     */
    AsyncFuture<Void> refreshDiscovery(final String id) {
        final List<AsyncFuture<List<URI>>> dynamic = new ArrayList<>();

        final List<URI> staticNodes = new ArrayList<>(this.staticNodes.get());

        if (!staticNodes.isEmpty()) {
            dynamic.add(async.resolved(staticNodes));
        }

        dynamic.add(discovery.find());
        return async.collect(dynamic).lazyTransform(refreshSweep(id));
    }

    /**
     * Operation that takes the existing list of clients, compares it to a collection of resolved
     * URIs and determines which nodes should be updated, and which should be removed.
     *
     * @param id id of the operation
     * @return a lazy transform
     */
    LazyTransform<Collection<List<URI>>, Void> refreshSweep(final String id) {
        return uriLists -> {
            final List<URI> uris = ImmutableList.copyOf(Iterables.concat(uriLists));

            final List<AsyncFuture<Update>> updated = new ArrayList<>();
            final List<RemovedNode> removedNodes = new ArrayList<>();

            final Map<URI, ClusterNode> oldClients = this.clients.get();

            if (oldClients == null) {
                log.warn("{}: Aborting refresh, shutting down", id);
                return async.resolved();
            }

            final Set<URI> removedUris = new HashSet<>(oldClients.keySet());

            for (final URI uri : uris) {
                final ClusterNode node = oldClients.get(uri);
                removedUris.remove(uri);

                if (node == null) {
                    /* first time URI has been seen, resolve new node */
                    updated.add(createClusterNode(id, uri));
                    continue;
                }

                /* re-query metadata for nodes already known and make sure it matches.
                 * if it does not match, create a new cluster node and close the old one.
                 * otherwise, re-use the existing node */
                updated.add(node.fetchMetadata().lazyTransform(m -> {
                    if (!node.metadata().equals(m)) {
                            /* add to removedNodes list to make sure it is being closed */
                        removedNodes.add(new RemovedNode(uri, node));
                        return createClusterNode(id, uri);
                    }

                    return async.resolved(new SuccessfulUpdate(uri, false, node));
                }).catchFailed(Update.error(uri)));
            }

            /* all the nodes that have not been seen in the updates list of uris have been removed
             * and should be closed */
            for (final URI uri : removedUris) {
                final ClusterNode remove = oldClients.get(uri);

                if (remove != null) {
                    removedNodes.add(new RemovedNode(uri, remove));
                }
            }

            return async
                .collect(updated)
                .lazyTransform(refreshLogAndPrepare(id, removedNodes, oldClients));
        };
    }

    /**
     * Operation the logs all intended operations and prepares for the final step.
     *
     * @param id id of the refresh operation
     * @param removed clients which should be removed
     * @param oldClients map of clients that should be replaced by a new map of clients
     * @return a lazy transform
     */
    LazyTransform<Collection<Update>, Void> refreshLogAndPrepare(
        final String id, final List<RemovedNode> removed, final Map<URI, ClusterNode> oldClients
    ) {
        return updates -> {
            final Set<ClusterNode> entries = new HashSet<>();
            final List<SuccessfulUpdate> ok = new ArrayList<>();
            final List<AsyncFuture<Void>> removals = new ArrayList<>();
            final Map<URI, ClusterNode> newClients = new HashMap<>();

            updates.forEach(update -> {
                update.handle(s -> {
                    if (!s.isAdded()) {
                        log.info("{} [new] {}", id, s.getUri());
                    }

                    newClients.put(s.getUri(), s.getNode());
                    entries.add(s.getNode());
                    ok.add(s);
                }, error -> {
                    log.error("{} [failed] {}", id, error.getUri(), error.getError());
                });
            });

            removed.forEach(remove -> {
                log.error("{} [remove] {}", id, remove.getUri());
                removals.add(remove.getNode().close());
            });

            if (entries.isEmpty() && useLocal) {
                log.info("{} [refresh] no nodes discovered, including local node", id);
                entries.add(local);
            }

            final Set<Map<String, String>> knownShards = extractKnownShards(entries);

            log.info("{} [update] {} {} result(s)", id, knownShards, entries.size());

            /* shutdown removed node */
            return async
                .collectAndDiscard(removals)
                .lazyTransform(refreshFinalize(id, oldClients, newClients, entries, ok));
        };
    }

    /**
     * Create a lazy transform that updates the local state of the registry, or attempts another
     * refresh if the local state has already been updated.
     *
     * @param id id of the operation
     * @param oldClients map of clients that should be updated from
     * @param newClients map of clients that should be updated to
     * @param entries entries to add to registry
     * @param ok list of successful updates
     * @return a lazy transform
     */
    LazyTransform<Void, Void> refreshFinalize(
        final String id, final Map<URI, ClusterNode> oldClients,
        final Map<URI, ClusterNode> newClients, final Set<ClusterNode> entries,
        final List<SuccessfulUpdate> ok
    ) {
        return v -> {
            if (this.clients.compareAndSet(oldClients, newClients)) {
                registry.getAndSet(
                    new NodeRegistry(async, new ArrayList<>(entries), entries.size()));
                return async.resolved();
            }

            log.warn("{} another refresh in progress, trying again", id);

            /* shutdown ok updates which are not part of the old collection */
            final List<AsyncFuture<Void>> shutdown = ok
                .stream()
                .filter(SuccessfulUpdate::isAdded)
                .map(s -> s.getNode().close())
                .collect(Collectors.toList());

            return async.collectAndDiscard(shutdown).lazyTransform(v0 -> refreshDiscovery(id));
        };
    }

    /**
     * A container that contains information about a node update.
     */
    interface Update {
        static Transform<Throwable, Update> error(final URI uri) {
            return e -> new FailedUpdate(uri, e);
        }

        /**
         * Handle the current update.
         *
         * @param successful Consumer for an successful update, will be called if the update is
         * successful
         * @param error Consumer for a failed update, will be called if the update is failed.
         */
        void handle(
            final Consumer<SuccessfulUpdate> successful, final Consumer<FailedUpdate> error
        );
    }

    /**
     * A successful node update.
     */
    @Data
    static class SuccessfulUpdate implements Update {
        /**
         * The URI that was updated.
         */
        private final URI uri;
        /**
         * If the update is a new addition.
         */
        private final boolean added;
        /**
         * The cluster node part of the update.
         */
        private final ClusterNode node;

        @Override
        public void handle(
            final Consumer<SuccessfulUpdate> successful, final Consumer<FailedUpdate> error
        ) {
            successful.accept(this);
        }
    }

    /**
     * A failed node update.
     */
    @Data
    static class FailedUpdate implements Update {
        /**
         * URI of the node that failed to update.
         */
        private final URI uri;
        /**
         * The error that caused the failure.
         */
        private final Throwable error;

        @Override
        public void handle(
            final Consumer<SuccessfulUpdate> successful, final Consumer<FailedUpdate> error
        ) {
            error.accept(this);
        }
    }

    /**
     * A single removed node.
     */
    @Data
    static class RemovedNode {
        /**
         * The URI of the removed node.
         */
        private final URI uri;
        /**
         * The cluster node that was removed.
         */
        private final ClusterNode node;
    }
}
