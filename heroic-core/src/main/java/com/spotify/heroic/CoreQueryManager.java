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

import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.DistributedAggregationCombiner;
import com.spotify.heroic.aggregation.Empty;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.grammar.DefaultScope;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.FunctionExpression;
import com.spotify.heroic.grammar.IntegerExpression;
import com.spotify.heroic.grammar.QueryExpression;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.grammar.RangeExpression;
import com.spotify.heroic.grammar.StringExpression;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryResultPart;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class CoreQueryManager implements QueryManager {
    public static final long SHIFT_TOLERANCE = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
    public static final QueryTrace.Identifier QUERY_NODE =
        QueryTrace.identifier(CoreQueryManager.class, "query_node");
    public static final QueryTrace.Identifier QUERY =
        QueryTrace.identifier(CoreQueryManager.class, "query");

    private final Features features;
    private final AsyncFramework async;
    private final ClusterManager cluster;
    private final QueryParser parser;
    private final QueryCache queryCache;
    private final AggregationFactory aggregations;
    private final OptionalLimit groupLimit;
    private boolean logQueries;
    private OptionalLimit logQueriesThresholdDataPoints;

    @Inject
    public CoreQueryManager(
        @Named("features") final Features features, final AsyncFramework async,
        final ClusterManager cluster, final QueryParser parser, final QueryCache queryCache,
        final AggregationFactory aggregations, @Named("groupLimit") final OptionalLimit groupLimit,
        @Named("logQueries") final boolean logQueries,
        @Named ("logQueriesThresholdDataPoints") final OptionalLimit logQueriesThresholdDataPoints
    ) {
        this.features = features;
        this.async = async;
        this.cluster = cluster;
        this.parser = parser;
        this.queryCache = queryCache;
        this.aggregations = aggregations;
        this.groupLimit = groupLimit;
        this.logQueries = logQueries;
        this.logQueriesThresholdDataPoints = logQueriesThresholdDataPoints;
    }

    @Override
    public QueryManager.Group useOptionalGroup(final Optional<String> group) {
        return new Group(cluster.useOptionalGroup(group));
    }

    @Override
    public QueryBuilder newQueryFromString(final String queryString) {
        final List<Expression> expressions = parser.parse(queryString);

        final Expression.Scope scope = new DefaultScope(System.currentTimeMillis());

        if (expressions.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one expression");
        }

        return expressions.get(0).eval(scope).visit(new Expression.Visitor<QueryBuilder>() {
            @Override
            public QueryBuilder visitQuery(final QueryExpression e) {
                final Optional<MetricType> source = e.getSource();

                final Optional<QueryDateRange> range =
                    e.getRange().map(expr -> expr.visit(new Expression.Visitor<QueryDateRange>() {
                        @Override
                        public QueryDateRange visitRange(final RangeExpression e) {
                            final long start =
                                e.getStart().cast(IntegerExpression.class).getValue();
                            final long end = e.getEnd().cast(IntegerExpression.class).getValue();

                            return new QueryDateRange.Absolute(start, end);
                        }
                    }));

                final Optional<Aggregation> aggregation =
                    e.getSelect().map(expr -> expr.visit(new Expression.Visitor<Aggregation>() {
                        @Override
                        public Aggregation visitFunction(final FunctionExpression e) {
                            return aggregations.build(e.getName(), e.getArguments(),
                                e.getKeywords());
                        }

                        @Override
                        public Aggregation visitString(final StringExpression e) {
                            return visitFunction(e.cast(FunctionExpression.class));
                        }
                    }));

                final Optional<Filter> filter = e.getFilter();

                return new QueryBuilder()
                    .source(source)
                    .range(range)
                    .aggregation(aggregation)
                    .filter(filter);
            }
        });
    }

    @RequiredArgsConstructor
    public class Group implements QueryManager.Group {
        private final List<ClusterShard> shards;

        @Override
        public AsyncFuture<QueryResult> query(Query q) {
            final List<AsyncFuture<QueryResultPart>> futures = new ArrayList<>();

            final MetricType source = q.getSource().orElse(MetricType.POINT);

            final QueryOptions options = q.getOptions().orElseGet(QueryOptions::defaults);
            final Aggregation aggregation = q.getAggregation().orElse(Empty.INSTANCE);
            final DateRange rawRange = buildRange(q);

            final long now = System.currentTimeMillis();

            final Filter filter = q.getFilter().orElseGet(TrueFilter::get);

            final AggregationContext context =
                AggregationContext.defaultInstance(cadenceFromRange(rawRange));
            final AggregationInstance root = aggregation.apply(context);

            final AggregationInstance aggregationInstance;

            final Features features = CoreQueryManager.this.features
                .applySet(q.getFeatures().orElseGet(FeatureSet::empty));

            boolean isDistributed = features.hasFeature(Feature.DISTRIBUTED_AGGREGATIONS);

            if (isDistributed) {
                aggregationInstance = root.distributed();
            } else {
                aggregationInstance = root;
            }

            final DateRange range = features.withFeature(Feature.SHIFT_RANGE,
                () -> buildShiftedRange(rawRange, aggregationInstance.cadence(), now),
                () -> rawRange);

            final AggregationCombiner combiner;

            if (isDistributed) {
                combiner = new DistributedAggregationCombiner(root.reducer(), range);
            } else {
                combiner = AggregationCombiner.DEFAULT;
            }

            final FullQuery.Request request =
                new FullQuery.Request(source, filter, range, aggregationInstance, options);

            return queryCache.load(request, () -> {
                for (final ClusterShard shard : shards) {
                    final AsyncFuture<QueryResultPart> queryPart = shard
                        .apply(g -> g.query(request))
                        .catchFailed(FullQuery.shardError(QUERY_NODE, shard))
                        .directTransform(QueryResultPart.fromResultGroup(shard));

                    futures.add(queryPart);
                }

                final OptionalLimit limit = options.getGroupLimit().orElse(groupLimit);

                return async.collect(futures,
                    QueryResult.collectParts(QUERY, range, combiner, limit,
                                         request,
                                         q.getRequestMetadata().orElse(new QueryRequestMetadata()),
                                         logQueries, logQueriesThresholdDataPoints));
            });
        }

        @Override
        public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
            return run(g -> g.findTags(request), FindTags::shardError, FindTags.reduce());
        }

        @Override
        public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
            return run(g -> g.findKeys(request), FindKeys::shardError, FindKeys.reduce());
        }

        @Override
        public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
            return run(g -> g.findSeries(request), FindSeries::shardError,
                FindSeries.reduce(request.getLimit()));
        }

        @Override
        public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
            return run(g -> g.deleteSeries(request), DeleteSeries::shardError,
                DeleteSeries.reduce());
        }

        @Override
        public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
            return run(g -> g.countSeries(request), CountSeries::shardError, CountSeries.reduce());
        }

        @Override
        public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
            return run(g -> g.tagKeyCount(request), TagKeyCount::shardError,
                TagKeyCount.reduce(request.getLimit(), request.getExactLimit()));
        }

        @Override
        public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
            return run(g -> g.tagSuggest(request), TagSuggest::shardError,
                TagSuggest.reduce(request.getLimit()));
        }

        @Override
        public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
            return run(g -> g.keySuggest(request), KeySuggest::shardError,
                KeySuggest.reduce(request.getLimit()));
        }

        @Override
        public AsyncFuture<TagValuesSuggest> tagValuesSuggest(
            final TagValuesSuggest.Request request
        ) {
            return run(g -> g.tagValuesSuggest(request), TagValuesSuggest::shardError,
                TagValuesSuggest.reduce(request.getLimit(), request.getGroupLimit()));
        }

        @Override
        public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
            return run(g -> g.tagValueSuggest(request), TagValueSuggest::shardError,
                TagValueSuggest.reduce(request.getLimit()));
        }

        @Override
        public AsyncFuture<WriteMetadata> writeSeries(final WriteMetadata.Request request) {
            return run(g -> g.writeSeries(request), WriteMetadata::shardError,
                WriteMetadata.reduce());
        }

        @Override
        public AsyncFuture<WriteMetric> writeMetric(final WriteMetric.Request write) {
            return run(g -> g.writeMetric(write), WriteMetric::shardError, WriteMetric.reduce());
        }

        @Override
        public List<ClusterShard> shards() {
            return shards;
        }

        private <T> AsyncFuture<T> run(
            final Function<ClusterNode.Group, AsyncFuture<T>> function,
            final Function<ClusterShard, Transform<Throwable, T>> catcher,
            final Collector<T, T> collector
        ) {
            final List<AsyncFuture<T>> futures = new ArrayList<>(shards.size());

            for (final ClusterShard shard : shards) {
                futures.add(shard.apply(function::apply).catchFailed(catcher.apply(shard)));
            }

            return async.collect(futures, collector);
        }

        private DateRange buildRange(Query q) {
            final long now = System.currentTimeMillis();

            return q
                .getRange()
                .map(r -> r.buildDateRange(now))
                .orElseThrow(() -> new IllegalArgumentException("Range must be present"));
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
            return Duration.of(Math.max(nominal, 1L), TimeUnit.MILLISECONDS);
        }

        return Duration.of(results.last(), TimeUnit.MILLISECONDS);
    }

    /**
     * Given a range and a cadence, return a range that might be shifted in case the end period is
     * too close or after 'now'. This is useful to avoid querying non-complete buckets.
     *
     * @param rawRange Original range.
     * @return A possibly shifted range.
     */
    DateRange buildShiftedRange(DateRange rawRange, long cadence, long now) {
        if (rawRange.getStart() > now) {
            throw new IllegalArgumentException("start is greater than now");
        }

        final DateRange rounded = rawRange.rounded(cadence);

        final long nowDelta = now - rounded.getEnd();

        if (nowDelta > SHIFT_TOLERANCE) {
            return rounded;
        }

        final long diff = Math.abs(Math.min(nowDelta, 0)) + SHIFT_TOLERANCE;

        return rounded.shift(-toleranceShiftPeriod(diff, cadence));
    }

    /**
     * Calculate a tolerance shift period that corresponds to the given difference that needs to be
     * applied to the range to honor the tolerance shift period.
     *
     * @param diff The time difference to apply.
     * @param cadence The cadence period.
     * @return The number of milliseconds that the query should be shifted to get within 'now' and
     * maintain the given cadence.
     */
    private long toleranceShiftPeriod(final long diff, final long cadence) {
        // raw query, only shift so that we are within now.
        if (cadence <= 0L) {
            return diff;
        }

        // Round up periods
        return ((diff + cadence - 1) / cadence) * cadence;
    }
}
