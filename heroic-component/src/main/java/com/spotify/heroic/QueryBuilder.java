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
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationQuery;
import com.spotify.heroic.aggregation.DefaultAggregationContext;
import com.spotify.heroic.aggregation.EmptyAggregation;
import com.spotify.heroic.aggregation.EmptyAggregationQuery;
import com.spotify.heroic.aggregation.GroupAggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.AggregationValue;
import com.spotify.heroic.grammar.FromDSL;
import com.spotify.heroic.grammar.QueryDSL;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.grammar.SelectDSL;
import com.spotify.heroic.metric.MetricType;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class QueryBuilder {
    private final AggregationFactory aggregations;
    private final FilterFactory filters;
    private final QueryParser parser;

    private String key;
    private Map<String, String> tags;
    private Filter filter;
    private List<String> groupBy;
    private DateRange range;
    private AggregationQuery aggregation = EmptyAggregationQuery.INSTANCE;
    private boolean disableCache;
    private String queryString;
    private MetricType source = MetricType.POINT;
    private AggregationContext context = new DefaultAggregationContext();

    /**
     * Specify a set of tags that has to match.
     *
     * @deprecated Use {@link #filter(Filter)} with the appropriate filter instead. These can be built using
     *             {@link FilterFactory#matchKey(String)}.
     */
    public QueryBuilder key(String key) {
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
    public QueryBuilder groupBy(List<String> groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    /**
     * Specify the date range for which data will be returned.
     * 
     * Note: This range might be rounded to accommodate the sampling period of a given aggregation.
     */
    public QueryBuilder range(DateRange range) {
        checkNotNull(range, "range must not be null");
        checkArgument(!range.isEmpty(), "range must not be empty");
        checkArgument(range.start() < range.end(), "range start must come before end");
        this.range = range;
        return this;
    }

    /**
     * Specify a filter to use.
     */
    public QueryBuilder filter(Filter filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Specify a query string to use, this will override most other aggregations and use the built-in
     * {@link QueryParser}.
     */
    public QueryBuilder queryString(String queryString) {
        this.queryString = queryString;
        return this;
    }

    /**
     * Specify an aggregation to use.
     */
    public QueryBuilder aggregationQuery(AggregationQuery aggregation) {
        this.aggregation = aggregation;
        return this;
    }

    public QueryBuilder source(MetricType source) {
        this.source = source;
        return this;
    }

    public QueryBuilder disableCache(boolean disableCache) {
        this.disableCache = disableCache;
        return this;
    }

    public QueryBuilder aggregationContext(AggregationContext context) {
        this.context = checkNotNull(context, "context");
        return this;
    }

    public Query build() {
        if (queryString != null) {
            return parseQueryString(queryString);
        }

        if (range == null) {
            throw new NullPointerException("range must not be null");
        }

        final Aggregation aggregation = legacyAggregation();
        final DateRange range = roundedRange(aggregation, this.range);
        final Filter filter = legacyFilter();

        if (filter instanceof Filter.True) {
            throw new IllegalArgumentException("a valid filter must be defined, not: true");
        }

        return new Query(filter, range, aggregation, disableCache, source);
    }

    Aggregation legacyAggregation() {
        return legacyAggregation(aggregation.build(context), groupBy);
    }

    Aggregation legacyAggregation(Aggregation aggregation, List<String> groupBy) {
        if (groupBy == null) {
            return aggregation;
        }

        return new GroupAggregation(groupBy, aggregation);
    }

    /**
     * Convert a MetricsRequest into a filter.
     *
     * This is meant to stay backwards compatible, since every filtering in MetricsRequest can be expressed as filter
     * objects.
     */
    Filter legacyFilter() {
        final List<Filter> statements = new ArrayList<>();

        if (tags != null && !tags.isEmpty()) {
            for (final Map.Entry<String, String> entry : tags.entrySet()) {
                statements.add(filters.matchTag(entry.getKey(), entry.getValue()));
            }
        }

        if (key != null) {
            statements.add(filters.matchKey(key));
        }

        if (filter != null) {
            statements.add(filter);
        }

        if (statements.isEmpty()) {
            return filters.t();
        }

        if (statements.size() == 1) {
            return statements.get(0).optimize();
        }

        return filters.and(statements).optimize();
    }

    Query parseQueryString(String query) {
        final QueryDSL q = parser.parseQuery(query);
        final FromDSL from = q.getSource();
        final SelectDSL select = q.getSelect();
        final Filter filter = q.getWhere().or(filters::t);

        final MetricType source = convertSource(from);

        final Aggregation aggregation = legacyAggregation(select.getAggregation().transform(customAggregation(select))
                .or(EmptyAggregation.INSTANCE), q.getGroupBy().transform((g) -> g.getGroupBy()).orNull());

        final DateRange range = roundedRange(aggregation, from.getRange().or(this::getRange));

        return new Query(filter, range, aggregation, true, source);
    }

    DateRange getRange() {
        if (range == null) {
            throw new IllegalStateException("No range configured");
        }

        return range;
    }

    MetricType convertSource(final FromDSL s) {
        final MetricType type = MetricType.fromIdentifier(s.getSource());

        if (type == null) {
            throw s.getContext().error("Invalid source: " + s.getSource());
        }

        return type;
    }

    Function<AggregationValue, Aggregation> customAggregation(final SelectDSL select) {
        return (a) -> {
            try {
                return aggregations.build(context, a.getName(), a.getArguments(), a.getKeywordArguments());
            } catch (final Exception e) {
                log.error("Failed to build aggregation", e);
                throw select.getContext().error(e);
            }
        };
    }

    /**
     * Check if the query wants to hint at a specific interval. If that is the case, round the provided date to the
     * specified interval.
     *
     * @param query
     * @return
     */
    DateRange roundedRange(Aggregation aggregation, DateRange range) {
        if (aggregation == null)
            return range;

        final long extent = aggregation.extent();

        if (extent == 0)
            return range;

        final DateRange result = range.rounded(extent).shiftStart(-extent);

        if (result.isEmpty())
            throw new IllegalArgumentException("range rounds to empty range with the aggregation: " + aggregation);

        return result;
    }
}