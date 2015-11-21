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

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.metric.MetricType;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class QueryBuilder {
    private final FilterFactory filters;

    private Optional<MetricType> source = Optional.empty();
    private Optional<Map<String, String>> tags = Optional.empty();
    private Optional<String> key = Optional.empty();
    private Optional<Filter> filter = Optional.empty();
    private Optional<List<String>> groupBy = Optional.empty();
    private Optional<QueryDateRange> range = Optional.empty();
    private Optional<Aggregation> aggregation = Optional.empty();
    private Optional<QueryOptions> options = Optional.empty();

    /**
     * Specify a set of tags that has to match.
     *
     * @deprecated Use {@link #filter(Filter)} with the appropriate filter instead. These can be
     *             built using {@link FilterFactory#matchKey(String)}.
     */
    public QueryBuilder key(Optional<String> key) {
        this.key = key;
        return this;
    }

    /**
     * Specify a set of tags that has to match.
     *
     * @deprecated Use {@link #filter(Filter)} with the appropriate filter instead. These can be
     *             built using {@link FilterFactory#matchTag(String, String)}.
     */
    public QueryBuilder tags(Optional<Map<String, String>> tags) {
        checkNotNull(tags, "tags must not be null");
        this.tags = pickOptional(this.tags, tags);
        return this;
    }

    /**
     * Specify a group by to use.
     *
     * @deprecated Use the group aggregation instead.
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
    public QueryBuilder range(Optional<QueryDateRange> range) {
        checkNotNull(range, "range");
        this.range = pickOptional(this.range, range).filter(r -> !r.isEmpty());
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
    public QueryBuilder aggregation(final Optional<Aggregation> aggregation) {
        checkNotNull(aggregation, "aggregation must not be null");
        this.aggregation = pickOptional(this.aggregation, aggregation);
        return this;
    }

    public QueryBuilder source(Optional<MetricType> source) {
        this.source = source;
        return this;
    }

    public QueryBuilder options(final Optional<QueryOptions> options) {
        checkNotNull(options, "options");
        this.options = pickOptional(this.options, options);
        return this;
    }

    public QueryBuilder rangeIfAbsent(final Optional<QueryDateRange> range) {
        if (!this.range.isPresent()) {
            return range(range);
        }

        return this;
    }

    public QueryBuilder optionsIfAbsent(final Optional<QueryOptions> options) {
        if (!this.options.isPresent()) {
            return options(options);
        }

        return this;
    }

    public Query build() {
        return new Query(aggregation, source, range, legacyFilter(), options, groupBy);
    }

    /**
     * Convert a MetricsRequest into a filter.
     *
     * This is meant to stay backwards compatible, since every filtering in MetricsRequest can be
     * expressed as filter objects.
     */
    Optional<Filter> legacyFilter() {
        final List<Filter> statements = new ArrayList<>();

        if (filter.isPresent()) {
            statements.add(filter.get());
        }

        if (tags.isPresent()) {
            for (final Map.Entry<String, String> entry : tags.get().entrySet()) {
                statements.add(filters.matchTag(entry.getKey(), entry.getValue()));
            }
        }

        if (key.isPresent()) {
            statements.add(filters.matchKey(key.get()));
        }

        if (statements.isEmpty()) {
            return Optional.empty();
        }

        if (statements.size() == 1) {
            return Optional.of(statements.get(0).optimize());
        }

        return Optional.of(filters.and(statements).optimize());
    }
}
