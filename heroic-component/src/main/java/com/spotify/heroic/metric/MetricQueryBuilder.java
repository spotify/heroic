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

package com.spotify.heroic.metric;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.GroupAggregation;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.AggregationValue;
import com.spotify.heroic.grammar.FromDSL;
import com.spotify.heroic.grammar.GroupByDSL;
import com.spotify.heroic.grammar.QueryDSL;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.grammar.QuerySource;
import com.spotify.heroic.grammar.SelectDSL;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Event;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.TimeData;

@RequiredArgsConstructor
public class MetricQueryBuilder {
    private final AggregationFactory aggregations;
    private final FilterFactory filters;
    private final QueryParser parser;

    private String backendGroup;
    private String key;
    private Map<String, String> tags;
    private Filter filter;
    private List<String> groupBy;
    private DateRange range;
    private Aggregation aggregation;
    private boolean disableCache;
    private String queryString;
    private Class<? extends TimeData> source = DataPoint.class;

    /**
     * Specify a set of tags that has to match.
     *
     * @deprecated Use {@link #filter(Filter)} with the appropriate filter instead. These can be built using
     *             {@link FilterFactory#matchKey(String)}.
     */
    public MetricQueryBuilder key(String key) {
        this.key = key;
        return this;
    }

    /**
     * Specify a set of tags that has to match.
     *
     * @deprecated Use {@link #filter(Filter)} with the appropriate filter instead. These can be built using
     *             {@link FilterFactory#matchTag(String, String)}.
     */
    public MetricQueryBuilder tags(Map<String, String> tags) {
        checkNotNull(tags, "tags must not be null");
        this.tags = tags;
        return this;
    }

    /**
     * Specify a group by to use.
     * 
     * @deprecated Use {@link #aggregation(Aggregation)} with the appropriate {@link GroupAggregation} instead.
     */
    public MetricQueryBuilder groupBy(List<String> groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    /**
     * Specify the date range for which data will be returned.
     * 
     * Note: This range might be rounded to accommodate the sampling period of a given aggregation.
     */
    public MetricQueryBuilder range(DateRange range) {
        checkNotNull(range, "range must not be null");
        checkArgument(!range.isEmpty(), "range must not be empty");
        checkArgument(range.start() < range.end(), "range start must come before end");
        this.range = range;
        return this;
    }

    /**
     * Specify a backend group to use.
     */
    public MetricQueryBuilder backendGroup(String backendGroup) {
        this.backendGroup = backendGroup;
        return this;
    }

    /**
     * Specify a filter to use.
     */
    public MetricQueryBuilder filter(Filter filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Specify a query string to use, this will override most other aggregations and use the built-in
     * {@link QueryParser}.
     */
    public MetricQueryBuilder queryString(String queryString) {
        this.queryString = queryString;
        return this;
    }

    /**
     * Specify an aggregation to use.
     */
    public MetricQueryBuilder aggregation(Aggregation aggregation) {
        this.aggregation = aggregation;
        return this;
    }

    public MetricQueryBuilder source(Class<? extends TimeData> source) {
        this.source = source;
        return this;
    }

    public MetricQueryBuilder disableCache(boolean disableCache) {
        this.disableCache = disableCache;
        return this;
    }

    public MetricQuery build() {
        if (queryString != null)
            return parseQuery(queryString);

        if (range == null)
            throw new NullPointerException("range must not be null");

        final DateRange range = roundedRange(this.aggregation, this.range);

        final Filter filter = legacyFilter();

        if (filter instanceof Filter.True)
            throw new IllegalArgumentException("a valid filter must be defined, not: true");

        return new MetricQuery(backendGroup, filter, groupBy, range, aggregation, disableCache, source);
    }

    /**
     * Convert a MetricsRequest into a filter.
     *
     * This is meant to stay backwards compatible, since every filtering in MetricsRequest can be expressed as filter
     * objects.
     */
    private Filter legacyFilter() {
        final List<Filter> statements = new ArrayList<>();

        if (tags != null && !tags.isEmpty()) {
            for (final Map.Entry<String, String> entry : tags.entrySet()) {
                statements.add(filters.matchTag(entry.getKey(), entry.getValue()));
            }
        }

        if (key != null)
            statements.add(filters.matchKey(key));

        if (filter != null)
            statements.add(filter);

        if (statements.size() == 0)
            return null;

        if (statements.size() == 1)
            return statements.get(0);

        return filters.and(statements).optimize();
    }

    private MetricQuery parseQuery(String query) {
        final QueryDSL q = parser.parseQuery(query);

        final FromDSL from = q.getSource();
        final SelectDSL select = q.getSelect();
        final Filter filter = q.getWhere();
        final GroupByDSL groupBy = q.getGroupBy();

        final Class<? extends TimeData> source = convertQuerySource(from);
        final Aggregation aggregation = convertQueryAggregation(select);
        final DateRange range = roundedRange(aggregation, from.getRange());
        final List<String> group = groupBy == null ? null : groupBy.getGroupBy();

        return new MetricQuery(null, filter, group, range, aggregation, true, source);
    }

    private Class<? extends TimeData> convertQuerySource(final FromDSL s) {
        if (s.getSource() == null)
            throw new IllegalArgumentException("source must be defined");

        if (s.getSource() == QuerySource.EVENTS)
            return Event.class;

        if (s.getSource() == QuerySource.SERIES)
            return DataPoint.class;

        throw new IllegalArgumentException("invalid source: " + s.getSource());
    }

    private Aggregation convertQueryAggregation(final SelectDSL select) {
        if (select.getAggregation() == null)
            return null;

        final AggregationValue a = select.getAggregation();

        try {
            return aggregations.build(a.getName(), a.getArguments(), a.getKeywordArguments());
        } catch (final Exception e) {
            throw select.getContext().error(e);
        }
    }

    /**
     * Check if the query wants to hint at a specific interval. If that is the case, round the provided date to the
     * specified interval.
     *
     * @param query
     * @return
     */
    private DateRange roundedRange(Aggregation aggregation, DateRange range) {
        if (aggregation == null)
            return range;

        final Sampling sampling = aggregation.sampling();

        if (sampling == null)
            return range;

        final DateRange result = range.rounded(sampling.getExtent()).rounded(sampling.getSize())
                .shiftStart(-sampling.getExtent());

        if (result.isEmpty())
            throw new IllegalArgumentException("range rounds to empty range with the aggregation: " + aggregation);

        return result;
    }
}