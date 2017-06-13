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
import lombok.Data;

@Data
public class QueryMetrics {
    private final Optional<String> query;
    private final Optional<Aggregation> aggregation;
    private final Optional<MetricType> source;
    private final Optional<QueryDateRange> range;
    private final Optional<Filter> filter;
    private final Optional<QueryOptions> options;
    private final Optional<JsonNode> clientContext;

    /* legacy state */
    private final Optional<String> key;
    private final Optional<Map<String, String>> tags;
    private final Optional<List<String>> groupBy;
    private final Optional<FeatureSet> features;

    public QueryMetrics(
        Optional<String> query, Optional<Aggregation> aggregation, Optional<MetricType> source,
        Optional<QueryDateRange> range, Optional<Filter> filter, Optional<QueryOptions> options,
        final Optional<JsonNode> clientContext
    ) {
        this.query = query;
        this.aggregation = aggregation;
        this.source = source;
        this.range = range;
        this.filter = filter;
        this.options = options;
        this.clientContext = clientContext;

        this.key = Optional.empty();
        this.tags = Optional.empty();
        this.groupBy = Optional.empty();
        this.features = Optional.empty();
    }

    @JsonCreator
    public QueryMetrics(
        @JsonProperty("query") Optional<String> query,
        @JsonProperty("aggregation") Optional<Aggregation> aggregation,
        @JsonProperty("aggregators") Optional<List<Aggregation>> aggregators,
        @JsonProperty("source") Optional<String> source,
        @JsonProperty("range") Optional<QueryDateRange> range,
        @JsonProperty("filter") Optional<Filter> filter,
        @JsonProperty("options") Optional<QueryOptions> options,
        @JsonProperty("clientContext") Optional<JsonNode> clientContext,
        @JsonProperty("key") Optional<String> key,
        @JsonProperty("tags") Optional<Map<String, String>> tags,
        @JsonProperty("groupBy") Optional<List<String>> groupBy,
        @JsonProperty("features") Optional<FeatureSet> features,
        /* ignored */ @JsonProperty("noCache") Boolean noCache
    ) {
        this.query = query;
        this.aggregation =
            firstPresent(aggregation, aggregators.filter(c -> !c.isEmpty()).map(Chain::fromList));
        this.source = source.flatMap(MetricType::fromIdentifier);
        this.range = range;
        this.filter = filter;
        this.options = options;
        this.clientContext = clientContext;

        this.key = key;
        this.tags = tags;
        this.groupBy = groupBy;
        this.features = features;
    }

    public QueryBuilder toQueryBuilder(final Function<String, QueryBuilder> stringToQuery) {
        final Supplier<? extends QueryBuilder> supplier = () -> {
            return new QueryBuilder()
                .key(key)
                .tags(tags)
                .groupBy(groupBy)
                .filter(filter)
                .range(range)
                .aggregation(aggregation)
                .source(source)
                .options(options)
                .clientContext(clientContext);
        };

        return query
            .map(stringToQuery)
            .orElseGet(supplier)
            .rangeIfAbsent(range)
            .optionsIfAbsent(options)
            .features(features);
    }
}
