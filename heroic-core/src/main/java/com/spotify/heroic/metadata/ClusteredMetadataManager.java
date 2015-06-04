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

package com.spotify.heroic.metadata;

import java.util.Map;

import javax.inject.Inject;

import lombok.ToString;

import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterManager.ClusterOperation;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.metadata.model.CountSeries;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.suggest.model.TagKeyCount;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@ToString
public class ClusteredMetadataManager {
    @Inject
    private AsyncFramework async;

    @Inject
    private ClusterManager cluster;

    @Inject
    private ClusteredMetadataManagerReporter reporter;

    public boolean isReady() {
        return cluster.isReady();
    }

    public <T> AsyncFuture<T> run(Map<String, String> tags, NodeCapability capability, ClusterOperation<T> op) {
        final NodeRegistryEntry node = cluster.findNode(tags, capability);

        if (node == null) {
            throw new RuntimeException("No node found matching " + tags + " with capability " + capability);
        }

        return op.run(node);
    }

    public AsyncFuture<FindTags> findTags(final String group, final RangeFilter filter) {
        return cluster.run(NodeCapability.QUERY, FindTags.reduce(), new ClusterOperation<FindTags>() {
            @Override
            public AsyncFuture<FindTags> run(final NodeRegistryEntry node) {
                return node.getClusterNode().useGroup(group).findTags(filter).error(FindTags.nodeError(node))
                        .onAny(reporter.reportFindTagsShard(node.getMetadata().getTags()));
            }
        }).onAny(reporter.reportFindTags());
    }

    public AsyncFuture<FindKeys> findKeys(final String group, final RangeFilter filter) {
        return cluster.run(NodeCapability.QUERY, FindKeys.reduce(), new ClusterOperation<FindKeys>() {
            @Override
            public AsyncFuture<FindKeys> run(NodeRegistryEntry node) {
                return node.getClusterNode().useGroup(group).findKeys(filter).error(FindKeys.nodeError(node))
                        .onAny(reporter.reportFindKeysShard(node.getMetadata().getTags()));
            }
        }).onAny(reporter.reportFindKeys());
    }

    public AsyncFuture<FindSeries> findSeries(final String group, final RangeFilter filter) {
        return cluster.run(NodeCapability.QUERY, FindSeries.reduce(filter.getLimit()),
                new ClusterOperation<FindSeries>() {
                    @Override
                    public AsyncFuture<FindSeries> run(NodeRegistryEntry node) {
                        return node.getClusterNode().useGroup(group).findSeries(filter)
                                .error(FindSeries.nodeError(node))
                                .onAny(reporter.reportFindSeriesShard(node.getMetadata().getTags()));
                    }
                }).onAny(reporter.reportFindSeries());
    }

    public AsyncFuture<TagKeyCount> tagKeyCount(final String group, final RangeFilter filter) {
        return cluster.run(NodeCapability.QUERY, TagKeyCount.reduce(filter.getLimit()),
                new ClusterOperation<TagKeyCount>() {
                    @Override
                    public AsyncFuture<TagKeyCount> run(NodeRegistryEntry node) {
                        return node.getClusterNode().useGroup(group).tagKeyCount(filter)
                                .error(TagKeyCount.nodeError(node));
                    }
                }).onAny(reporter.reportTagKeySuggest());
    }

    public AsyncFuture<CountSeries> countSeries(final String group, final RangeFilter filter) {
        return cluster.run(NodeCapability.QUERY, CountSeries.reduce(), new ClusterOperation<CountSeries>() {
            @Override
            public AsyncFuture<CountSeries> run(NodeRegistryEntry node) {
                return node.getClusterNode().useGroup(group).countSeries(filter).error(CountSeries.nodeError(node));
            }
        }).onAny(reporter.reportCount());
    }

    public AsyncFuture<DeleteSeries> deleteSeries(final String group, final RangeFilter filter) {
        return cluster.run(NodeCapability.WRITE, DeleteSeries.reduce(), new ClusterOperation<DeleteSeries>() {
            @Override
            public AsyncFuture<DeleteSeries> run(NodeRegistryEntry node) {
                return node.getClusterNode().useGroup(group).deleteSeries(filter);
            }
        }).onAny(reporter.reportDeleteSeries());
    }

    public AsyncFuture<WriteResult> write(final String group, final DateRange range, final Series series) {
        return cluster.run(series.getTags(), NodeCapability.WRITE, WriteResult.merger(),
                new ClusterOperation<WriteResult>() {
                    @Override
                    public AsyncFuture<WriteResult> run(NodeRegistryEntry node) {
                        return node.getClusterNode().useGroup(group).writeSeries(range, series);
                    }
                }).onAny(reporter.reportWrite());
    }
}
