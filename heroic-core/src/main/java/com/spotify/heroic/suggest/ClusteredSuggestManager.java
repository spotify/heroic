package com.spotify.heroic.suggest;

import java.util.List;

import javax.inject.Inject;

import lombok.ToString;

import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterManager.ClusterOperation;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.suggest.model.KeySuggest;
import com.spotify.heroic.suggest.model.MatchOptions;
import com.spotify.heroic.suggest.model.TagSuggest;
import com.spotify.heroic.suggest.model.TagValueSuggest;
import com.spotify.heroic.suggest.model.TagValuesSuggest;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@ToString
public class ClusteredSuggestManager {
    @Inject
    private AsyncFramework async;

    @Inject
    private ClusterManager cluster;

    @Inject
    private ClusteredMetadataManagerReporter reporter;

    public boolean isReady() {
        return cluster.isReady();
    }

    public AsyncFuture<TagSuggest> tagSuggest(final String group, final RangeFilter filter, final MatchOptions match,
            final String key, final String value) {
        return cluster.run(NodeCapability.QUERY, TagSuggest.reduce(filter.getLimit()),
                new ClusterOperation<TagSuggest>() {
                    @Override
                    public AsyncFuture<TagSuggest> run(NodeRegistryEntry node) {
                        return node.getClusterNode().useGroup(group).tagSuggest(filter, match, key, value)
                                .error(TagSuggest.nodeError(node));
                    }
                }).onAny(reporter.reportTagSuggest());
    }

    public AsyncFuture<KeySuggest> keySuggest(final String group, final RangeFilter filter, final MatchOptions match,
            final String key) {
        return cluster.run(NodeCapability.QUERY, KeySuggest.reduce(filter.getLimit()),
                new ClusterOperation<KeySuggest>() {
                    @Override
                    public AsyncFuture<KeySuggest> run(NodeRegistryEntry node) {
                        return node.getClusterNode().useGroup(group).keySuggest(filter, match, key)
                                .error(KeySuggest.nodeError(node));
                    }
                }).onAny(reporter.reportKeySuggest());
    }

    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(final String group, final RangeFilter filter,
            final List<String> exclude, final int groupLimit) {
        return cluster.run(NodeCapability.QUERY, TagValuesSuggest.reduce(filter.getLimit(), groupLimit),
                new ClusterOperation<TagValuesSuggest>() {
                    @Override
                    public AsyncFuture<TagValuesSuggest> run(NodeRegistryEntry node) {
                        return node.getClusterNode().useGroup(group).tagValuesSuggest(filter, exclude, groupLimit)
                                .error(TagValuesSuggest.nodeError(node));
                    }
                }).onAny(reporter.reportTagValuesSuggest());
    }

    public AsyncFuture<TagValueSuggest> tagValueSuggest(final String group, final RangeFilter filter, final String key) {
        return cluster.run(NodeCapability.QUERY, TagValueSuggest.reduce(filter.getLimit()),
                new ClusterOperation<TagValueSuggest>() {
                    @Override
                    public AsyncFuture<TagValueSuggest> run(NodeRegistryEntry node) {
                        return node.getClusterNode().useGroup(group).tagValueSuggest(filter, key)
                                .error(TagValueSuggest.nodeError(node));
                    }
                }).onAny(reporter.reportTagValueSuggest());
    }
}
