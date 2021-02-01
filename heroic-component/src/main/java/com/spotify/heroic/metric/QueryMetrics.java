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

import static com.spotify.heroic.common.Optionals.firstPresent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import com.spotify.heroic.QueryBuilder;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.Chain;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.filter.Filter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

@AutoValue
public abstract class QueryMetrics {
    public static QueryMetrics create(
        Optional<String> query,
        Optional<Aggregation> aggregation,
        Optional<MetricType> metricType,
        Optional<QueryDateRange> range,
        Optional<Filter> filter,
        Optional<QueryOptions> options,
        Optional<JsonNode> clientContext
    ) {
        return legacyCreate(query, aggregation, metricType, range, filter, options, clientContext,
             Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), false);
    }

    @JsonCreator
    public static QueryMetrics legacyCreate(
        @JsonProperty("query") Optional<String> query,
        @JsonProperty("aggregation") Optional<Aggregation> aggregation,
        @JsonProperty("metricType") Optional<MetricType> metricType,
        @JsonProperty("range") Optional<QueryDateRange> range,
        @JsonProperty("filter") Optional<Filter> filter,
        @JsonProperty("options") Optional<QueryOptions> options,
        @JsonProperty("clientContext") Optional<JsonNode> clientContext,
        // legacy options
        @JsonProperty("aggregators") Optional<List<Aggregation>> aggregators,
        @JsonProperty("key") Optional<String> key,
        @JsonProperty("tags") Optional<Map<String, String>> tags,
        @JsonProperty("features") Optional<FeatureSet> features,
        /* ignored */ @JsonProperty("noCache") Boolean noCache
    ) {

        final Optional<Aggregation> legitAggregation = firstPresent(aggregation,
            aggregators.filter(c -> !c.isEmpty()).map(Chain::fromList));

        return new AutoValue_QueryMetrics(query, legitAggregation, metricType, range, filter,
            options, clientContext, key, tags, features);
    }

    @JsonProperty("query")
    public abstract Optional<String> query();
    @JsonProperty("aggregation")
    public abstract Optional<Aggregation> aggregation();
    @JsonProperty("metricType")
    public abstract Optional<MetricType> metricType();
    @JsonProperty("range")
    public abstract Optional<QueryDateRange> range();
    @JsonProperty("filter")
    public abstract Optional<Filter> filter();
    @JsonProperty("options")
    public abstract Optional<QueryOptions> options();
    @JsonProperty("clientContext")
    public abstract Optional<JsonNode> clientContext();

    /* legacy state */
    @JsonProperty("key")
    public abstract Optional<String> key();
    @JsonProperty("tags")
    public abstract Optional<Map<String, String>> tags();
    @JsonProperty("features")
    public abstract Optional<FeatureSet> features();

    public QueryBuilder toQueryBuilder(final Function<String, QueryBuilder> stringToQuery) {
        final Supplier<? extends QueryBuilder> supplier = () -> new QueryBuilder()
            .key(key())
            .tags(tags())
            .filter(filter())
            .range(range())
            .aggregation(aggregation())
            .metricType(metricType())
            .options(options())
            .clientContext(clientContext());

        return query()
            .map(stringToQuery)
            .orElseGet(supplier)
            .rangeIfAbsent(range())
            .optionsIfAbsent(options())
            .features(features());
    }
}
