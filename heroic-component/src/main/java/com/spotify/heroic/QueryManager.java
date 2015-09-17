package com.spotify.heroic;

import java.util.Collection;

import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.metric.QueryResult;

import eu.toolchain.async.AsyncFuture;

public interface QueryManager {
    public Group useGroup(String group);

    public Collection<? extends Group> useGroupPerNode(String group);

    public Group useDefaultGroup();

    public Collection<? extends Group> useDefaultGroupPerNode();

    public QueryBuilder newQuery();

    public QueryBuilder newQueryFromString(String query);

    public AsyncFuture<Void> initialized();

    public interface Group extends Iterable<ClusterNode.Group> {
        public AsyncFuture<QueryResult> query(Query query);

        public ClusterNode.Group first();
    }
}