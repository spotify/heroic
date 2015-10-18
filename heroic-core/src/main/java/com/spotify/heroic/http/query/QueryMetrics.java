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

import static java.util.Optional.ofNullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.AggregationQuery;
import com.spotify.heroic.aggregation.ChainAggregationQuery;
import com.spotify.heroic.aggregation.EmptyAggregationQuery;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;

import lombok.Data;

@Data
public class QueryMetrics {
    private static final QueryDateRange DEFAULT_DATE_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);
    private static final Map<String, String> DEFAULT_TAGS = new HashMap<String, String>();
    private static final boolean DEFAULT_NO_CACHE = false;

    private final Optional<String> query;
    private final Optional<String> key;
    private final Map<String, String> tags;
    private final Optional<Filter> filter;
    private final Optional<List<String>> groupBy;
    private final QueryDateRange range;
    private final boolean noCache;
    private final AggregationQuery aggregators;
    private final MetricType source;

    @JsonCreator
    public QueryMetrics(@JsonProperty("query") String query, @JsonProperty("key") String key,
            @JsonProperty("tags") Map<String, String> tags, @JsonProperty("filter") Filter filter,
            @JsonProperty("groupBy") List<String> groupBy, @JsonProperty("range") QueryDateRange range,
            @JsonProperty("noCache") Boolean noCache,
            @JsonProperty("aggregators") List<AggregationQuery> aggregators,
            @JsonProperty("source") String source) {
        this.query = ofNullable(query);
        this.key = ofNullable(key);
        this.tags = ofNullable(tags).orElse(DEFAULT_TAGS);
        this.filter = ofNullable(filter);
        this.groupBy = ofNullable(groupBy);
        this.range = ofNullable(range).orElse(DEFAULT_DATE_RANGE);
        this.noCache = ofNullable(noCache).orElse(DEFAULT_NO_CACHE);
        this.aggregators = ofNullable(aggregators).filter(c -> !c.isEmpty())
                .<AggregationQuery> map(chain -> new ChainAggregationQuery(chain))
                .orElseGet(EmptyAggregationQuery::new);
        this.source = MetricType.fromIdentifier(source)
                .orElseThrow(() -> new IllegalArgumentException("Not a valid source: " + source));
    }
}
