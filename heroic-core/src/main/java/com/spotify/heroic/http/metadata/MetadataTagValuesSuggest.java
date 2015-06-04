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

import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.impl.TrueFilterImpl;
import com.spotify.heroic.http.query.QueryDateRange;
import com.spotify.heroic.model.DateRange;

@Data
public class MetadataTagValuesSuggest {
    private static final Filter DEFAULT_FILTER = TrueFilterImpl.get();
    private static final int DEFAULT_LIMIT = 10;
    private static final QueryDateRange DEFAULT_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);
    private static final List<String> DEFAULT_EXCLUDE = ImmutableList.of();
    private static final int DEFAULT_GROUP_LIMIT = 10;

    /**
     * Filter the suggestions being returned.
     */
    private final Filter filter;

    /**
     * Limit the number of suggestions being returned.
     */
    private final int limit;

    /**
     * Query for tags within the given range.
     */
    private final DateRange range;

    /**
     * Exclude the given tags from the result.
     */
    private final List<String> exclude;

    /**
     * Limit the number of values a single suggestion group may contain.
     */
    private final int groupLimit;

    @JsonCreator
    public MetadataTagValuesSuggest(@JsonProperty("filter") Filter filter, @JsonProperty("limit") Integer limit,
            @JsonProperty("range") QueryDateRange range, @JsonProperty("exclude") List<String> exclude,
            @JsonProperty("groupLimimt") Integer groupLimit) {
        this.filter = Optional.fromNullable(filter).or(DEFAULT_FILTER);
        this.limit = Optional.fromNullable(limit).or(DEFAULT_LIMIT);
        this.range = Optional.fromNullable(range).or(DEFAULT_RANGE).buildDateRange();
        this.exclude = Optional.fromNullable(exclude).or(DEFAULT_EXCLUDE);
        this.groupLimit = Optional.fromNullable(groupLimit).or(DEFAULT_GROUP_LIMIT);
    }

    public static MetadataTagValuesSuggest createDefault() {
        return new MetadataTagValuesSuggest(null, null, null, null, null);
    }
}