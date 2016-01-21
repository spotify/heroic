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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.Aggregations;
import com.spotify.heroic.aggregation.Group;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;

import java.util.List;
import java.util.Optional;

import lombok.Data;

@Data
public class Query {
    private final Optional<Aggregation> aggregation;
    private final Optional<MetricType> source;
    private final Optional<QueryDateRange> range;
    private final Optional<Filter> filter;
    private final Optional<QueryOptions> options;
    private final Optional<List<String>> groupBy;

    @JsonCreator
    public Query(@JsonProperty("aggregators") final Optional<List<Aggregation>> aggregators,
            @JsonProperty("aggregation") final Optional<Aggregation> aggregation,
            @JsonProperty("source") final Optional<MetricType> source,
            @JsonProperty("range") final Optional<QueryDateRange> range,
            @JsonProperty("filter") final Optional<Filter> filter,
            @JsonProperty("options") final Optional<QueryOptions> options,
            @JsonProperty("groupBy") final Optional<List<String>> groupBy) {
        this.filter = filter;
        this.range = range;
        this.aggregation =
                Optionals.pickOptional(aggregation, aggregators.flatMap(Aggregations::chain));
        this.source = source;
        this.options = options;
        this.groupBy = groupBy;
    }

    public Optional<Aggregation> aggregation() {
        if (groupBy.isPresent()) {
            return aggregation.<Aggregation> map(a -> new Group(groupBy, Optional.of(a)));
        }

        return aggregation;
    }
}
