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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationQuery;
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

    private Optional<String> key = Optional.absent();
    private Map<String, String> tags = ImmutableMap.of();
    private Optional<Filter> filter = Optional.absent();
    private Optional<List<String>> groupBy = Optional.absent();
    private Optional<DateRange> range = Optional.absent();
    private MetricType source = MetricType.POINT;
    private AggregationQuery aggregationQuery = EmptyAggregationQuery.INSTANCE;
    private Optional<Function<AggregationContext, Aggregation>> aggregationBuilder = Optional.absent();
    private AggregationContext context = new DefaultAggregationContext();
    private Optional<QueryOptions> options = Optional.absent();

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
    public QueryBuilder groupBy(Optional<List<String>> groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    /**
     * Specify the date range for which data will be returned.
     * 
     * Note: This range might be rounded to accommodate the sampling period of a given aggregation.
     */
    public QueryBuilder range(Optional<DateRange> range) {
        if (range.isPresent()) {
            checkArgument(!range.get().isEmpty(), "range must not be empty");
        }

        this.range = range;
        return this;
    }

    /**
     * Specify a filter to use.
     */
    public QueryBuilder filter(Optional<Filter> filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Specify an aggregation to use.
     */
    public QueryBuilder aggregationQuery(AggregationQuery aggregation) {
        this.aggregationQuery = aggregation;
        return this;
    }

    public QueryBuilder aggregationBuilder(Optional<Function<AggregationContext, Aggregation>> aggregationBuilder) {
        this.aggregationBuilder = aggregationBuilder;
        return this;
    }

    public QueryBuilder source(MetricType source) {
        this.source = source;
        return this;
    }

    public QueryBuilder options(QueryOptions options) {
        this.options = Optional.of(options);
        return this;
    }

    public QueryBuilder aggregationContext(AggregationContext context) {
        this.context = checkNotNull(context, "context");
        return this;
    }

    public Query build() {
        final Filter filter = legacyFilter();

        final Aggregation aggregation = legacyGroupBy(
                aggregationBuilder.transform(b -> b.apply(context)).or(() -> aggregationQuery.build(context)));

        final Optional<DateRange> range = roundedRange(aggregation);

        if (filter instanceof Filter.True) {
            throw new IllegalArgumentException(
                    "A valid filter must be defined, not one that matches everything (true)");
        }

        if (!range.isPresent()) {
            throw new IllegalStateException("Range is not specified");
        }

        final QueryOptions options = this.options.or(QueryOptions::defaults);

        return new Query(filter, range.get(), aggregation, source, options);
    }

    Aggregation legacyGroupBy(final Aggregation aggregation) {
        return groupBy.<Aggregation> transform(g -> new GroupAggregation(g, aggregation)).or(aggregation);
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

    Optional<DateRange> roundedRange(Aggregation aggregation) {
        return range.transform(r -> {
            final long extent = aggregation.extent();

            if (extent == 0) {
                return r;
            }

            return r.rounded(extent).shiftStart(-extent);
        });
    }
}