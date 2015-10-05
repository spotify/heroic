package com.spotify.heroic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.AggregationValue;
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
        final Optional<Function<AggregationContext, Aggregation>> aggregationBuilder = select.getAggregation().transform(customAggregation(select));

        /* incorporate legacy group by */
        final Optional<List<String>> groupBy = q.getGroupBy().transform((g) -> g.getGroupBy());

        return newQuery().source(source).range(from.getRange()).aggregationBuilder(aggregationBuilder).filter(filter)
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

            for (ClusterNode.Group group : groups) {
                final ClusterNode c = group.node();
                futures.add(
                        group.query(q.getSource(), q.getFilter(), q.getRange(), q.getAggregation(), q.getOptions())
                                .catchFailed(ResultGroups.nodeError(QUERY_NODE, group))
                                .directTransform(QueryResultPart.fromResultGroup(q.getRange(), c)));
            }

            return async.collect(futures, QueryResult.collectParts(QUERY, q.getRange()));
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
        final MetricType type = MetricType.fromIdentifier(s.getSource());

        if (type == null) {
            throw s.getContext().error("Invalid source: " + s.getSource());
        }

        return type;
    }

    Function<AggregationValue, Function<AggregationContext, Aggregation>> customAggregation(final SelectDSL select) {
        return (a) -> {
            return (context) -> {
                try {
                    return aggregations.build(context, a.getName(), a.getArguments(), a.getKeywordArguments());
                } catch (final Exception e) {
                    log.error("Failed to build aggregation", e);
                    throw select.getContext().error(e);
                }
            };
        };
    }
}