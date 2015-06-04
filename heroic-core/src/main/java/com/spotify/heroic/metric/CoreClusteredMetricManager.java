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

package com.spotify.heroic.metric;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import lombok.ToString;

import com.google.common.collect.Lists;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metric.model.ResultGroups;
import com.spotify.heroic.metric.model.ShardedResultGroups;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@ToString(of = {})
public class CoreClusteredMetricManager implements ClusteredMetricManager, LifeCycle {
    @Inject
    protected AsyncFramework async;

    @Inject
    protected ClusteredMetricManagerReporter reporter;

    @Inject
    protected ClusterManager cluster;

    @Inject
    protected FilterFactory filters;

    @Inject
    protected AggregationFactory aggregations;

    @Inject
    protected QueryParser parser;

    /**
     * Buffer a write to this backend, will block if the buffer is full.
     *
     * @param write
     *            The write to buffer.
     * @throws InterruptedException
     *             If the write was interrupted.
     * @throws BufferEnqueueException
     *             If the provided metric could not be buffered.
     */
    public AsyncFuture<WriteResult> write(final String group, WriteMetric write) {
        final NodeRegistryEntry node = cluster.findNode(write.getSeries().getTags(), NodeCapability.WRITE);
        return node.getClusterNode().useGroup(group).writeMetric(write).onAny(reporter.reportWrite());
    }

    @Override
    public AsyncFuture<MetricResult> query(MetricQuery request) {
        final Collection<NodeRegistryEntry> nodes = cluster.findAllShards(NodeCapability.QUERY);

        if (nodes.isEmpty())
            throw new IllegalStateException("no query nodes found");

        return doQuery(request, nodes);
    }

    protected AsyncFuture<MetricResult> doQuery(MetricQuery request, final Collection<NodeRegistryEntry> nodes) {
        final List<AsyncFuture<ShardedResultGroups>> callbacks = setupQueries(request, nodes);

        return async.collect(callbacks, ShardedResultGroups.merger())
                .transform(toQueryMetricsResult(request.getRange())).onAny(reporter.reportQuery());
    }

    private List<AsyncFuture<ShardedResultGroups>> setupQueries(MetricQuery request,
            final Collection<NodeRegistryEntry> nodes) {
        final List<AsyncFuture<ShardedResultGroups>> callbacks = Lists.newArrayList();

        for (final NodeRegistryEntry n : nodes) {
            final Map<String, String> shard = n.getMetadata().getTags();

            final AsyncFuture<ResultGroups> query = executeQueryOn(request, n.getClusterNode()).onAny(
                    reporter.reportShardFullQuery(n.getMetadata()));

            callbacks.add(query.transform(ShardedResultGroups.toSharded(shard)).error(
                    ShardedResultGroups.nodeError(n.getMetadata().getId(), n.getClusterNode().toString(), shard)));
        }

        return callbacks;
    }

    @Override
    public AsyncFuture<MetricResult> queryOnNode(final MetricQuery request, final NodeRegistryEntry n) {
        final Map<String, String> shard = n.getMetadata().getTags();

        final AsyncFuture<ResultGroups> query = executeQueryOn(request, n.getClusterNode()).onAny(
                reporter.reportShardFullQuery(n.getMetadata()));

        return query.transform(ShardedResultGroups.toSharded(shard))
                .error(ShardedResultGroups.nodeError(n.getMetadata().getId(), n.getClusterNode().toString(), shard))
                .transform(toQueryMetricsResult(request.getRange())).onAny(reporter.reportQuery());
    }

    /**
     * Execute this request on the given {@link NodeRegistryEntry}.
     */
    private AsyncFuture<ResultGroups> executeQueryOn(MetricQuery request, ClusterNode node) {
        return node.useGroup(request.getBackendGroup()).query(request.getSource(), request.getFilter(),
                request.getGroupBy(), request.getRange(), request.getAggregation(), request.isDisableCache());
    }

    private Transform<ShardedResultGroups, MetricResult> toQueryMetricsResult(final DateRange rounded) {
        return new Transform<ShardedResultGroups, MetricResult>() {
            @Override
            public MetricResult transform(ShardedResultGroups result) throws Exception {
                return new MetricResult(rounded, result);
            }
        };
    }

    public MetricQueryBuilder newRequest() {
        return new MetricQueryBuilder(aggregations, filters, parser);
    }

    @Override
    public AsyncFuture<Void> start() throws Exception {
        return async.resolved();
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
        return async.resolved();
    }

    @Override
    public boolean isReady() {
        return false;
    }
}