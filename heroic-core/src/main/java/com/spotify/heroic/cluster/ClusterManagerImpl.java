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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.async.MaybeError;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.scheduler.Task;
import com.spotify.heroic.statistics.HeroicReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Transform;

/**
 * Handles management of cluster state.
 *
 * The primary responsibility is to receive refresh requests through {@link #refresh()} that should cause the cluster
 * state to be updated.
 *
 * It also provides an interface for looking up nodes through {@link #findNode(Map, NodeCapability)}.
 *
 * @author udoprog
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@ToString(of = { "useLocal" })
public class ClusterManagerImpl implements ClusterManager, LifeCycle {
    @Inject
    private AsyncFramework async;

    @Inject
    private ClusterDiscovery discovery;

    @Inject
    private NodeMetadata localMetadata;

    @Inject
    private Map<String, RpcProtocol> protocols;

    @Inject
    private Scheduler scheduler;

    @Inject
    @Named("useLocal")
    private Boolean useLocal;

    @Inject
    @Named("local")
    private NodeRegistryEntry localEntry;

    @Inject
    @Named("topology")
    private Set<Map<String, String>> topology;

    @Inject
    private HeroicReporter reporter;

    @Inject
    @Named("oneshot")
    private boolean oneshot;

    @Inject
    private HeroicContext context;

    private final AtomicReference<Set<URI>> staticNodes = new AtomicReference<Set<URI>>(new HashSet<URI>());

    final AtomicReference<NodeRegistry> registry = new AtomicReference<>(null);

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
    public NodeRegistryEntry findNode(final Map<String, String> tags, NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        return registry.findEntry(tags, capability);
    }

    @Override
    public Collection<NodeRegistryEntry> findAllShards(NodeCapability capability) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            throw new IllegalStateException("Registry not ready");
        }

        final Collection<NodeRegistryEntry> all = registry.findAllShards(capability);

        if (!topology.isEmpty()) {
            int found = 0;

            for (final NodeRegistryEntry entry : all) {
                if (topology.contains(entry.getMetadata().getTags()))
                    found += 1;
            }

            if (found != topology.size())
                throw new IllegalStateException(String.format(
                        "Could not find %s nodes for the whole topology (%s), found (%s)", capability,
                        StringUtils.join(topology, ", "), StringUtils.join(topologyOf(all), ", ")));
        }

        return all;
    }

    @Override
    public <T> AsyncFuture<T> run(NodeCapability capability, Collector<T, T> reducer, ClusterOperation<T> op) {
        final Collection<NodeRegistryEntry> nodes = findAllShards(capability);

        final List<AsyncFuture<T>> requests = new ArrayList<>(nodes.size());

        for (final NodeRegistryEntry node : nodes)
            requests.add(op.run(node));

        return async.collect(requests, reducer);
    }

    @Override
    public <T> AsyncFuture<T> run(Map<String, String> tags, NodeCapability capability, Collector<T, T> reducer,
            ClusterOperation<T> op) {
        final NodeRegistryEntry node = findNode(tags, capability);

        final List<AsyncFuture<T>> requests = new ArrayList<>(1);
        requests.add(op.run(node));
        return async.collect(requests, reducer);
    }

    private List<Map<String, String>> topologyOf(Collection<NodeRegistryEntry> entries) {
        final List<Map<String, String>> shards = new ArrayList<>();

        for (final NodeRegistryEntry e : entries)
            shards.add(e.getMetadata().getTags());

        return shards;
    }

    @Override
    public AsyncFuture<Void> refresh() {
        final AsyncFuture<List<MaybeError<NodeRegistryEntry>>> transform;

        if (discovery instanceof ClusterDiscoveryModule.Null) {
            log.info("No discovery mechanism configured, using local node");
            final List<MaybeError<NodeRegistryEntry>> results = new ArrayList<>();

            if (useLocal) {
                results.add(MaybeError.just(localEntry));
            }

            transform = async.resolved(results);
        } else {
            log.info("Refreshing cluster");
            transform = discovery.find().transform(resolve());
        }

        return addStaticNodes(transform).transform(updateRegistry());
    }

    private static Collector<List<MaybeError<NodeRegistryEntry>>, List<MaybeError<NodeRegistryEntry>>> joinMaybeErrors = new Collector<List<MaybeError<NodeRegistryEntry>>, List<MaybeError<NodeRegistryEntry>>>() {
        @Override
        public List<MaybeError<NodeRegistryEntry>> collect(Collection<List<MaybeError<NodeRegistryEntry>>> results)
                throws Exception {
            final List<MaybeError<NodeRegistryEntry>> result = new ArrayList<>();

            for (final List<MaybeError<NodeRegistryEntry>> r : results)
                result.addAll(r);

            return result;
        }
    };

    private AsyncFuture<List<MaybeError<NodeRegistryEntry>>> addStaticNodes(
            final AsyncFuture<List<MaybeError<NodeRegistryEntry>>> transform) {
        final List<URI> staticNodes = new ArrayList<>(this.staticNodes.get());

        final AsyncFuture<List<MaybeError<NodeRegistryEntry>>> finalTransform;

        if (!staticNodes.isEmpty()) {
            final List<AsyncFuture<List<MaybeError<NodeRegistryEntry>>>> transforms = ImmutableList.of(transform, async
                    .resolved(staticNodes).transform(resolve()));
            finalTransform = async.collect(transforms, joinMaybeErrors);
        } else {
            finalTransform = transform;
        }

        return finalTransform;
    }

    private static Collector<MaybeError<NodeRegistryEntry>, List<MaybeError<NodeRegistryEntry>>> joinMaybeErrorsCollection = new Collector<MaybeError<NodeRegistryEntry>, List<MaybeError<NodeRegistryEntry>>>() {
        @Override
        public List<MaybeError<NodeRegistryEntry>> collect(Collection<MaybeError<NodeRegistryEntry>> results)
                throws Exception {
            return new ArrayList<>(results);
        }
    };

    private LazyTransform<List<URI>, List<MaybeError<NodeRegistryEntry>>> resolve() {
        return new LazyTransform<List<URI>, List<MaybeError<NodeRegistryEntry>>>() {
            @Override
            public AsyncFuture<List<MaybeError<NodeRegistryEntry>>> transform(final List<URI> nodes) throws Exception {
                final List<AsyncFuture<MaybeError<NodeRegistryEntry>>> callbacks = new ArrayList<>(nodes.size());

                for (final URI uri : nodes) {
                    callbacks.add(resolve(uri).transform(MaybeError.<NodeRegistryEntry> transformJust()).error(
                            handleError(uri)));
                }

                return async.collect(callbacks, joinMaybeErrorsCollection);
            }
        };
    }

    private LazyTransform<List<MaybeError<NodeRegistryEntry>>, Void> updateRegistry() {
        return new LazyTransform<List<MaybeError<NodeRegistryEntry>>, Void>() {
            @Override
            public AsyncFuture<Void> transform(List<MaybeError<NodeRegistryEntry>> results) throws Exception {
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

                log.info("Updated registry: {} result(s), {} failure(s)", entries.size(), failures.size());
                final NodeRegistry oldRegistry = registry.getAndSet(new NodeRegistry(async, new ArrayList<>(entries),
                        results.size()));

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

    @Override
    public ClusterManager.Statistics getStatistics() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            return null;
        }

        return new ClusterManager.Statistics(registry.getOnlineNodes(), registry.getOfflineNodes());
    }

    @Override
    public AsyncFuture<Void> start() throws Exception {
        final AsyncFuture<Void> startup;

        if (!oneshot) {
            startup = this.context.startedFuture().transform(new Transform<Void, Void>() {
                @Override
                public Void transform(Void result) throws Exception {
                    scheduler.periodically("cluster-refresh", 1, TimeUnit.MINUTES, new Task() {
                        @Override
                        public void run() throws Exception {
                            refresh().get();
                        }
                    });

                    return null;
                }
            });
        } else {
            startup = this.context.startedFuture();
        }

        startup.transform(new LazyTransform<Void, Void>() {
            @Override
            public AsyncFuture<Void> transform(Void result) throws Exception {
                return refresh().error(new Transform<Throwable, Void>() {
                    @Override
                    public Void transform(Throwable e) throws Exception {
                        log.error("initial metadata refresh failed", e);
                        return null;
                    }
                });
            }
        });

        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
        return async.resolved(null);
    }

    @Override
    public boolean isReady() {
        final NodeRegistry registry = this.registry.get();

        if (registry == null) {
            return false;
        }

        return registry.getOnlineNodes() > 0;
    }

    private final LazyTransform<ClusterNode, NodeRegistryEntry> localTransform = new LazyTransform<ClusterNode, NodeRegistryEntry>() {
        @Override
        public AsyncFuture<NodeRegistryEntry> transform(final ClusterNode node) throws Exception {
            if (useLocal && localMetadata.getId().equals(node.metadata().getId())) {
                log.info("Using local instead of {}", node);

                return node.close().transform(new Transform<Void, NodeRegistryEntry>() {
                    @Override
                    public NodeRegistryEntry transform(Void result) throws Exception {
                        return localEntry;
                    }
                });
            }

            return async.resolved(new NodeRegistryEntry(node, node.metadata()));
        }
    };

    /**
     * Resolve the given URI to a specific protocol implementation (then use it, if found).
     * 
     * @param uri
     *            The uri to resolve.
     * @return A future containing a node registry entry.
     */
    private AsyncFuture<NodeRegistryEntry> resolve(final URI uri) {
        final RpcProtocol protocol = protocols.get(uri.getScheme());

        if (protocol == null)
            throw new IllegalArgumentException("Unsupported scheme '" + uri.getScheme());

        return protocol.connect(uri).transform(localTransform);
    }
}
