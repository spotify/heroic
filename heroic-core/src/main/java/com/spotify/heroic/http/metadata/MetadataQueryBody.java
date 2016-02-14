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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataQueryBody {
    private static final int DEFAULT_LIMIT = 50;

    /**
     * Only include time series which match the exact key.
     */
    private final Optional<String> matchKey;

    /**
     * Only include time series which matches the exact key/value combination.
     */
    private final Optional<Map<String, String>> matchTags;

    /**
     * Only include time series which has the following tags.
     */
    private final Optional<Set<String>> hasTags;

    /**
     * A general set of filters. If this is combined with the other mechanisms, all the filters will
     * be AND:ed together.
     */
    private final Optional<Filter> filter;

    /**
     * The date range to query for.
     */
    private final Optional<QueryDateRange> range;

    private final int limit;

    public Filter makeFilter(FilterFactory filters) {
        final List<Filter> statements = new ArrayList<>();

        if (filter.isPresent()) {
            statements.add(filter.get());
        }

        if (matchTags.isPresent()) {
            matchTags
                .get()
                .entrySet()
                .forEach(e -> statements.add(filters.matchTag(e.getKey(), e.getValue())));
        }

        if (hasTags.isPresent()) {
            hasTags.get().forEach(t -> statements.add(filters.hasTag(t)));
        }

        if (matchKey.isPresent()) {
            statements.add(filters.matchKey(matchKey.get()));
        }

        if (statements.size() == 0) {
            return filters.t();
        }

        if (statements.size() == 1) {
            return statements.get(0).optimize();
        }

        return filters.and(statements).optimize();
    }

    @JsonCreator
    public MetadataQueryBody(
        @JsonProperty("matchKey") Optional<String> matchKey,
        @JsonProperty("matchTags") Optional<Map<String, String>> matchTags,
        @JsonProperty("hasTags") Optional<Set<String>> hasTags,
        @JsonProperty("filter") Optional<Filter> filter,
        @JsonProperty("range") Optional<QueryDateRange> range,
        @JsonProperty("limit") Optional<Integer> limit
    ) {
        this.matchKey = matchKey;
        this.matchTags = matchTags;
        this.hasTags = hasTags;
        this.filter = filter;
        this.range = range;
        this.limit = limit.orElse(DEFAULT_LIMIT);
    }
}
