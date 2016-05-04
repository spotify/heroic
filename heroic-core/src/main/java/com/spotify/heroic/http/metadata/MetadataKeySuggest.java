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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.suggest.MatchOptions;
import lombok.Data;

import java.util.Optional;

@Data
public class MetadataKeySuggest {
    private static final int DEFAULT_LIMIT = 10;
    private static final MatchOptions DEFAULT_MATCH = MatchOptions.builder().fuzzy(false).build();

    private final Optional<Filter> filter;
    private final Optional<QueryDateRange> range;
    private final int limit;
    private final MatchOptions match;
    private final Optional<String> key;

    @JsonCreator
    public MetadataKeySuggest(
        @JsonProperty("filter") Optional<Filter> filter, @JsonProperty("key") Optional<String> key,
        @JsonProperty("limit") Optional<Integer> limit,
        @JsonProperty("range") Optional<QueryDateRange> range,
        @JsonProperty("match") Optional<MatchOptions> match
    ) {
        this.filter = filter;
        this.range = range;
        this.limit = limit.orElse(DEFAULT_LIMIT);
        this.match = match.orElse(DEFAULT_MATCH);
        this.key = key;
    }
}
