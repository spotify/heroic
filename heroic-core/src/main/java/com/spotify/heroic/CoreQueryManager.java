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
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.inject.Inject;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.DefaultAggregationContext;
import com.spotify.heroic.aggregation.Empty;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryResultPart;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.ResultGroups;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

public class CoreQueryManager implements QueryManager {
    public static final QueryTrace.Identifier QUERY_NODE =
            QueryTrace.identifier(CoreQueryManager.class, "query_node");
    public static final QueryTrace.Identifier QUERY =
            QueryTrace.identifier(CoreQueryManager.class, "query");

    @Inject
    private AsyncFramework async;

    @Inject
    private ClusterManager cluster;

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
        final Query q = parser.parseQuery(queryString);

        /* get aggregation that is part of statement, if any */
        final Optional<Aggregation> aggregation = q.getAggregation();

        return newQuery().source(q.getSource()).range(q.getRange()).aggregation(aggregation)
                .filter(q.getFilter());
    }

    @Override
    public String queryToString(final Query q) {
        return parser.stringifyQuery(q);
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

            final MetricType source = q.getSource().orElse(MetricType.POINT);

            final QueryOptions options = q.getOptions().orElseGet(QueryOptions::defaults);

            final Aggregation aggregation = q.getAggregation().orElse(Empty.INSTANCE);
            final DateRange rawRange = buildRange(q);
            final Duration cadence = buildCadence(aggregation, rawRange);
            final DateRange range = rawRange.rounded(cadence.toMilliseconds());
            final Filter filter = q.getFilter().orElseGet(filters::t);

            final AggregationContext context = new DefaultAggregationContext(cadence);
            final AggregationInstance root = aggregation.apply(context);

            final AggregationInstance aggregationInstance = root.distributed();
            final AggregationCombiner combiner = root.combiner(range);

            for (ClusterNode.Group group : groups) {
                final ClusterNode c = group.node();

                final AsyncFuture<QueryResultPart> queryPart =
                        group.query(source, filter, range, aggregationInstance, options)
                                .catchFailed(ResultGroups.nodeError(QUERY_NODE, group))
                                .directTransform(QueryResultPart.fromResultGroup(range, c));

                futures.add(queryPart);
            }

            return async.collect(futures, QueryResult.collectParts(QUERY, range, combiner));
        }

        private Duration buildCadence(final Aggregation aggregation, final DateRange rawRange) {
            return aggregation.size().map(Duration::ofMilliseconds)
                    .orElseGet(() -> cadenceFromRange(rawRange));
        }

        private DateRange buildRange(Query q) {
            final long now = System.currentTimeMillis();

            return q.getRange().map(r -> r.buildDateRange(now))
                    .orElseThrow(() -> new QueryStateException("Range must be present"));
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

    private static final SortedSet<Long> INTERVAL_FACTORS =
            ImmutableSortedSet.of(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS.convert(5, TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS.convert(10, TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS.convert(50, TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS.convert(100, TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS.convert(250, TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS.convert(500, TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS),
                    TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS),
                    TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS),
                    TimeUnit.MILLISECONDS.convert(15, TimeUnit.SECONDS),
                    TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS),
                    TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES),
                    TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES),
                    TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES),
                    TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES),
                    TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES),
                    TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS),
                    TimeUnit.MILLISECONDS.convert(3, TimeUnit.HOURS),
                    TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS),
                    TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS),
                    TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS),
                    TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS),
                    TimeUnit.MILLISECONDS.convert(3, TimeUnit.DAYS),
                    TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS),
                    TimeUnit.MILLISECONDS.convert(14, TimeUnit.DAYS));

    public static final long INTERVAL_GOAL = 240;

    private Duration cadenceFromRange(final DateRange range) {
        final long diff = range.diff();
        final long nominal = diff / INTERVAL_GOAL;

        final SortedSet<Long> results = INTERVAL_FACTORS.headSet(nominal);

        if (results.isEmpty()) {
            return Duration.of(nominal, TimeUnit.MILLISECONDS);
        }

        return Duration.of(results.last(), TimeUnit.MILLISECONDS);
    }
}
