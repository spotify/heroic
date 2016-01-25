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

package com.spotify.heroic.http.query;

import static com.spotify.heroic.common.Optionals.firstPresent;
import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.Chain;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import jersey.repackaged.com.google.common.collect.ImmutableSet;
import lombok.Data;

@Data
public class QueryMetrics {
    private final Optional<String> query;
    private final Optional<Aggregation> aggregation;
    private final Optional<MetricType> source;
    private final Optional<QueryDateRange> range;
    private final Optional<Filter> filter;
    private final Optional<QueryOptions> options;

    /* legacy state */
    private final Optional<String> key;
    private final Optional<Map<String, String>> tags;
    private final Optional<List<String>> groupBy;
    private final Set<String> features;

    public QueryMetrics(Optional<String> query, Optional<Aggregation> aggregation,
            Optional<MetricType> source, Optional<QueryDateRange> range, Optional<Filter> filter,
            Optional<QueryOptions> options) {
        this.query = query;
        this.aggregation = aggregation;
        this.source = source;
        this.range = range;
        this.filter = filter;
        this.options = options;

        this.key = Optional.empty();
        this.tags = Optional.empty();
        this.groupBy = Optional.empty();
        this.features = ImmutableSet.of();
    }

    @JsonCreator
    public QueryMetrics(@JsonProperty("query") String query,
            @JsonProperty("aggregation") Aggregation aggregation,
            @JsonProperty("aggregators") List<Aggregation> aggregators,
            @JsonProperty("source") String source, @JsonProperty("range") QueryDateRange range,
            @JsonProperty("filter") Filter filter, @JsonProperty("key") String key,
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("groupBy") List<String> groupBy,
            @JsonProperty("options") QueryOptions options,
            @JsonProperty("features") Set<String> features,
            /* ignored */ @JsonProperty("noCache") Boolean noCache) {
        this.query = ofNullable(query);
        this.aggregation = firstPresent(ofNullable(aggregation),
                ofNullable(aggregators).filter(c -> !c.isEmpty()).map(Chain::new));
        this.source = MetricType.fromIdentifier(source);
        this.range = ofNullable(range);
        this.filter = ofNullable(filter);
        this.options = ofNullable(options);

        this.key = ofNullable(key);
        this.tags = ofNullable(tags);
        this.groupBy = ofNullable(groupBy);
        this.features = ofNullable(features).orElseGet(ImmutableSet::of);
    }
}
