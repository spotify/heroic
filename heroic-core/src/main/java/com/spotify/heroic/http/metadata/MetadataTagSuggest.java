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

package com.spotify.heroic.http.metadata;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.suggest.MatchOptions;

import lombok.Data;

@Data
public class MetadataTagSuggest {
    private static final int DEFAULT_LIMIT = 10;
    private static final MatchOptions DEFAULT_MATCH = MatchOptions.builder().fuzzy(false).build();

    private final Optional<Filter> filter;
    private final int limit;
    private final Optional<QueryDateRange> range;
    private final MatchOptions match;
    private final String key;
    private final String value;

    @JsonCreator
    public MetadataTagSuggest(@JsonProperty("filter") Filter filter,
            @JsonProperty("limit") Integer limit, @JsonProperty("range") QueryDateRange range,
            @JsonProperty("match") MatchOptions match, @JsonProperty("key") String key,
            @JsonProperty("value") String value) {
        this.filter = Optional.ofNullable(filter);
        this.range = Optional.ofNullable(range);
        this.limit = Optional.ofNullable(limit).orElse(DEFAULT_LIMIT);
        this.match = Optional.ofNullable(match).orElse(DEFAULT_MATCH);
        this.key = key;
        this.value = value;
    }

    public static MetadataTagSuggest createDefault() {
        return new MetadataTagSuggest(null, null, null, null, null, null);
    }
}
