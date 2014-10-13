package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.async.Reducer;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;

@Slf4j
public class ClusteredMetadataManager {
    @Inject
    private ClusterManager cluster;

    @Inject
    private ClusteredMetadataManagerReporter reporter;

    public boolean isReady() {
        return cluster.isReady();
    }

    public static interface ClusterOperation<T> {
        public Future<T> run(NodeRegistryEntry node);
    }

    /**
     * TODO Remove short path for v4 when all of the cluster is v5.
     *
     * @param capability
     * @param reducer
     * @param op
     * @return
     */
    public <T> Future<T> run(NodeCapability capability, Reducer<T, T> reducer, ClusterOperation<T> op) {
        final Collection<NodeRegistryEntry> nodes = cluster.findAllShards(capability);

        final List<Future<T>> requests = new ArrayList<>(nodes.size());

        if (cluster.isAnyV(nodes, 4)) {
            log.warn("Using short path because we found one v4 node");
            return op.run(nodes.iterator().next());
        }

        for (final NodeRegistryEntry node : nodes) {
            requests.add(op.run(node));
        }

        return Futures.reduce(requests, reducer);
    }

    public <T> Future<T> run(Map<String, String> tags, NodeCapability capability, ClusterOperation<T> op) {
        final NodeRegistryEntry node = cluster.findNode(tags, capability);

        if (node == null) {
            throw new RuntimeException("No node found matching: " + tags);
        }

        return op.run(node);
    }

    public Future<FindTags> findTags(final Filter filter) {
        return run(NodeCapability.QUERY, FindTags.reduce(), new ClusterOperation<FindTags>() {
            @Override
            public Future<FindTags> run(NodeRegistryEntry node) {
                return node.getClusterNode().findTags(filter);
            }
        }).register(reporter.reportFindTags());
    }

    public Future<FindKeys> findKeys(final Filter filter) {
        return run(NodeCapability.QUERY, FindKeys.reduce(), new ClusterOperation<FindKeys>() {
            @Override
            public Future<FindKeys> run(NodeRegistryEntry node) {
                return node.getClusterNode().findKeys(filter);
            }
        }).register(reporter.reportFindKeys());
    }

    public Future<FindSeries> findSeries(final Filter filter) {
        return run(NodeCapability.QUERY, FindSeries.reduce(), new ClusterOperation<FindSeries>() {
            @Override
            public Future<FindSeries> run(NodeRegistryEntry node) {
                return node.getClusterNode().findSeries(filter);
            }
        }).register(reporter.reportFindSeries());
    }

    public Future<DeleteSeries> deleteSeries(final Filter filter) {
        return run(NodeCapability.WRITE, DeleteSeries.reduce(), new ClusterOperation<DeleteSeries>() {
            @Override
            public Future<DeleteSeries> run(NodeRegistryEntry node) {
                return node.getClusterNode().deleteSeries(filter);
            }
        }).register(reporter.reportDeleteSeries());
    }

    public Future<String> write(final Series series) {
        return run(series.getTags(), NodeCapability.WRITE, new ClusterOperation<String>() {
            @Override
            public Future<String> run(NodeRegistryEntry node) {
                return node.getClusterNode().writeSeries(series);
            }
        }).register(reporter.reportWrite());
    }
}
