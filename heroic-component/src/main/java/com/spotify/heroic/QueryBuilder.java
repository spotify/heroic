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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.heroic.common.Optionals.pickOptional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.DefaultAggregationContext;
import com.spotify.heroic.aggregation.EmptyAggregationQuery;
import com.spotify.heroic.aggregation.GroupAggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryOptions;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class QueryBuilder {
    private final FilterFactory filters;

    private Map<String, String> tags = ImmutableMap.of();
    private MetricType source = MetricType.POINT;

    private Optional<String> key = Optional.empty();
    private Optional<Filter> filter = Optional.empty();
    private Optional<List<String>> groupBy = Optional.empty();
    private Optional<DateRange> range = Optional.empty();
    private Optional<Function<AggregationContext, Aggregation>> aggregation = Optional.empty();
    private Optional<AggregationContext> context = Optional.empty();
    private Optional<QueryOptions> options = Optional.empty();

    /**
     * Specify a set of tags that has to match.
     *
     * @deprecated Use {@link #filter(Filter)} with the appropriate filter instead. These can be built using
     *             {@link FilterFactory#matchKey(String)}.
     */
    public QueryBuilder key(Optional<String> key) {
        this.key = key;
        return this;
    }

    /**
     * Specify a set of tags that has to match.
     *
     * @deprecated Use {@link #filter(Filter)} with the appropriate filter instead. These can be built using
     *             {@link FilterFactory#matchTag(String, String)}.
     */
    public QueryBuilder tags(Map<String, String> tags) {
        checkNotNull(tags, "tags must not be null");
        this.tags = tags;
        return this;
    }

    /**
     * Specify a group by to use.
     * 
     * @deprecated Use {@link #aggregation(Aggregation)} with the appropriate {@link GroupAggregation} instead.
     */
    public QueryBuilder groupBy(final Optional<List<String>> groupBy) {
        checkNotNull(groupBy, "groupBy must not be null");
        this.groupBy = pickOptional(this.groupBy, groupBy);
        return this;
    }

    /**
     * Specify the date range for which data will be returned.
     * 
     * Note: This range might be rounded to accommodate the sampling period of a given aggregation.
     */
    public QueryBuilder range(Optional<DateRange> range) {
        checkNotNull(range, "range");
        this.range = pickOptional(this.range, range).filter(DateRange::isNotEmpty);
        return this;
    }

    /**
     * Specify a filter to use.
     */
    public QueryBuilder filter(final Optional<Filter> filter) {
        checkNotNull(filter, "filter must not be null");
        this.filter = pickOptional(this.filter, filter);
        return this;
    }

    /**
     * Specify an aggregation to use.
     */
    public QueryBuilder aggregation(final Optional<Function<AggregationContext, Aggregation>> aggregation) {
        checkNotNull(aggregation, "aggregation must not be null");
        this.aggregation = pickOptional(this.aggregation, aggregation);
        return this;
    }

    public QueryBuilder source(MetricType source) {
        this.source = source;
        return this;
    }

    public QueryBuilder options(final Optional<QueryOptions> options) {
        checkNotNull(options, "options");
        this.options = pickOptional(this.options, options);
        return this;
    }

    public QueryBuilder rangeIfAbsent(final Optional<DateRange> range) {
        if (!this.range.isPresent()) {
            return range(range);
        }

        return this;
    }

    public Query build() {
        final Filter filter = legacyFilter();

        if (!range.isPresent()) {
            throw new IllegalStateException("Range is not specified");
        }

        final DateRange range = this.range.get();

        final Aggregation aggregation = buildAggregation(range);

        final DateRange roundedRange = roundedRange(range, aggregation);

        final QueryOptions options = this.options.orElseGet(QueryOptions::defaults);

        return new Query(filter, roundedRange, aggregation, source, options);
    }

    private Aggregation buildAggregation(final DateRange range) {
        final AggregationContext ctx = context.orElseGet(calculateFromRange(range));
        return legacyGroupBy(this.aggregation.orElse(EmptyAggregationQuery.INSTANCE::build).apply(ctx));
    }

    private static final SortedSet<Long> INTERVAL_FACTORS = ImmutableSortedSet.of(
        TimeUnit.MILLISECONDS.convert(1, TimeUnit.MILLISECONDS),
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
        TimeUnit.MILLISECONDS.convert(14, TimeUnit.DAYS)
    );

    public static final long INTERVAL_GOAL = 240;

    private Supplier<AggregationContext> calculateFromRange(final DateRange range) {
        return () -> {
            final long diff = range.diff();
            final long nominal = diff / INTERVAL_GOAL;

            final SortedSet<Long> results = INTERVAL_FACTORS.headSet(nominal);

            if (results.isEmpty()) {
                return new DefaultAggregationContext(nominal);
            }

            return new DefaultAggregationContext(results.last());
        };
    }

    Aggregation legacyGroupBy(final Aggregation aggregation) {
        return groupBy.<Aggregation> map(g -> new GroupAggregation(g, aggregation)).orElse(aggregation);
    }

    /**
     * Convert a MetricsRequest into a filter.
     *
     * This is meant to stay backwards compatible, since every filtering in MetricsRequest can be expressed as filter
     * objects.
     */
    Filter legacyFilter() {
        final List<Filter> statements = new ArrayList<>();

        if (!tags.isEmpty()) {
            for (final Map.Entry<String, String> entry : tags.entrySet()) {
                statements.add(filters.matchTag(entry.getKey(), entry.getValue()));
            }
        }

        if (key.isPresent()) {
            statements.add(filters.matchKey(key.get()));
        }

        if (filter.isPresent()) {
            statements.add(filter.get());
        }

        if (statements.isEmpty()) {
            return filters.t();
        }

        if (statements.size() == 1) {
            return statements.get(0).optimize();
        }

        return filters.and(statements).optimize();
    }

    DateRange roundedRange(final DateRange range, final Aggregation aggregation) {
        final long extent = aggregation.extent();

        if (extent == 0) {
            return range;
        }

        return range.rounded(extent);
    }
}