package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.model.Series;

public class ClusteredMetadataManager {
    @Inject
    private ClusterManager cluster;

    public boolean isReady() {
        return cluster.isReady();
    }

    public static interface ClusterOperation<T> {
        public Callback<T> run(NodeRegistryEntry node);
    }

    public <T> Callback<T> run(NodeCapability capability,
            Callback.Reducer<T, T> reducer, ClusterOperation<T> op) {
        final Collection<NodeRegistryEntry> nodes = cluster
                .findAllShards(capability);

        final List<Callback<T>> requests = new ArrayList<>(nodes.size());

        for (final NodeRegistryEntry node : nodes) {
            requests.add(op.run(node));
        }

        return ConcurrentCallback.newReduce(requests, reducer);
    }

    public <T> Callback<T> run(Map<String, String> tags,
            NodeCapability capability, ClusterOperation<T> op) {
        final NodeRegistryEntry node = cluster.findNode(tags, capability);

        if (node == null) {
            throw new RuntimeException("No node found matching: " + tags);
        }

        return op.run(node);
    }

    public Callback<FindTags> findTags(final Filter filter) {
        return run(NodeCapability.QUERY, FindTags.reduce(),
                new ClusterOperation<FindTags>() {
            @Override
            public Callback<FindTags> run(NodeRegistryEntry node) {
                return node.getClusterNode().findTags(filter);
            }
        });
    }

    public Callback<FindKeys> findKeys(final Filter filter) {
        return run(NodeCapability.QUERY, FindKeys.reduce(),
                new ClusterOperation<FindKeys>() {
            @Override
            public Callback<FindKeys> run(NodeRegistryEntry node) {
                return node.getClusterNode().findKeys(filter);
            }
        });
    }

    public Callback<FindSeries> findSeries(final Filter filter) {
        return run(NodeCapability.QUERY, FindSeries.reduce(),
                new ClusterOperation<FindSeries>() {
            @Override
            public Callback<FindSeries> run(NodeRegistryEntry node) {
                return node.getClusterNode().findSeries(filter);
            }
        });
    }

    public Callback<DeleteSeries> deleteSeries(final Filter filter) {
        return run(NodeCapability.WRITE, DeleteSeries.reduce(),
                new ClusterOperation<DeleteSeries>() {
            @Override
            public Callback<DeleteSeries> run(NodeRegistryEntry node) {
                return node.getClusterNode().deleteSeries(filter);
            }
        });
    }

    public Callback<String> write(final Series series) {
        return run(series.getTags(), NodeCapability.WRITE,
                new ClusterOperation<String>() {
                    @Override
                    public Callback<String> run(NodeRegistryEntry node) {
                        return node.getClusterNode().writeSeries(series);
                    }
                });
    }
}
