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

package com.spotify.heroic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.FromDSL;
import com.spotify.heroic.grammar.QueryDSL;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.grammar.SelectDSL;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryResultPart;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.ResultGroups;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoreQueryManager implements QueryManager {
    public static final QueryTrace.Identifier QUERY_NODE = QueryTrace.identifier(CoreQueryManager.class, "query_node");
    public static final QueryTrace.Identifier QUERY = QueryTrace.identifier(CoreQueryManager.class, "query");

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
        return new QueryBuilder(filters);
    }

    @Override
    public QueryBuilder newQueryFromString(final String queryString) {
        final QueryDSL q = parser.parseQuery(queryString);

        final FromDSL from = q.getSource();
        final MetricType source = convertSource(from);
        final SelectDSL select = q.getSelect();
        final Optional<Filter> filter = q.getWhere();

        /* get aggregation that is part of statement, if any */
        final Optional<Aggregation> aggregationBuilder = select.getAggregation()
                .map(a -> a.build(aggregations));

        /* incorporate legacy group by */
        final Optional<List<String>> groupBy = q.getGroupBy().map((g) -> g.getGroupBy());

        return newQuery().source(source).range(from.getRange()).aggregation(aggregationBuilder).filter(filter)
                .groupBy(groupBy);
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

            final AggregationInstance root = q.getAggregation();
            final AggregationInstance aggregation = root.distributed();
            final AggregationCombiner combiner = root.combiner(q.getRange());

            for (ClusterNode.Group group : groups) {
                final ClusterNode c = group.node();
                futures.add(
                        group.query(q.getSource(), q.getFilter(), q.getRange(), aggregation, q.getOptions())
                                .catchFailed(ResultGroups.nodeError(QUERY_NODE, group))
                                .directTransform(QueryResultPart.fromResultGroup(q.getRange(), c)));
            }

            return async.collect(futures, QueryResult.collectParts(QUERY, q.getRange(), combiner));
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

    MetricType convertSource(final FromDSL s) {
        return MetricType.fromIdentifier(s.getSource())
                .orElseThrow(() -> s.getContext().error("Invalid source: " + s.getSource()));
    }
}