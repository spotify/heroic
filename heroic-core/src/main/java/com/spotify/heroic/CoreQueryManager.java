package com.spotify.heroic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryResultPart;
import com.spotify.heroic.metric.ResultGroups;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

public class CoreQueryManager implements QueryManager {
    @Inject
    private AsyncFramework async;

    @Inject
    private ClusterManager cluster;

    @Inject
    private AggregationFactory aggregations;

    @Inject
    private FilterFactory filters;

    @Inject
    private QueryParser parser;

    @Override
    public Group useGroup(String group) {
        return new Group(cluster.useGroup(group));
    }

    @Override
    public Collection<Group> useGroupPerNode(String group) {
        final List<Group> result = new ArrayList<>();

        for (ClusterNode.Group g : cluster.useGroup(group)) {
            result.add(new Group(ImmutableList.of(g)));
        }

        return result;
    }

    @Override
    public Group useDefaultGroup() {
        return new Group(cluster.useDefaultGroup());
    }

    @Override
    public Collection<Group> useDefaultGroupPerNode() {
        final List<Group> result = new ArrayList<>();

        for (ClusterNode.Group g : cluster.useDefaultGroup()) {
            result.add(new Group(ImmutableList.of(g)));
        }

        return result;
    }

    @Override
    public QueryBuilder newQuery() {
        return new QueryBuilder(aggregations, filters, parser);
    }

    @Override
    public AsyncFuture<Void> initialized() {
        return cluster.initialized();
    }

    @RequiredArgsConstructor
    public class Group implements QueryManager.Group {
        private final Iterable<ClusterNode.Group> groups;

        @Override
        public AsyncFuture<QueryResult> query(Query q) {
            final List<AsyncFuture<QueryResultPart>> futures = new ArrayList<>();

            for (ClusterNode.Group group : groups) {
                final ClusterNode c = group.node();
                futures.add(
                        group.query(q.getSource(), q.getFilter(), q.getRange(), q.getAggregation(), q.isDisableCache())
                                .catchFailed(ResultGroups.nodeError(group))
                                .transform(QueryResultPart.fromResultGroup(q.getRange(), c)));
            }

            return async.collect(futures, QueryResult.collectParts(q.getRange()));
        }

        @Override
        public ClusterNode.Group first() {
            return groups.iterator().next();
        }

        @Override
        public Iterator<ClusterNode.Group> iterator() {
            return groups.iterator();
        }
    }
}