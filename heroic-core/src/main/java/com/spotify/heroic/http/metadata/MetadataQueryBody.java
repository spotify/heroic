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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.filter.Filter;
import lombok.Data;
import lombok.NonNull;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataQueryBody {
    public static final int DEFAULT_LIMIT = 50;

    /**
     * Only include time series which match the exact key.
     */
    @NonNull
    private final Optional<String> matchKey;

    /**
     * Only include time series which matches the exact key/value combination.
     */
    @NonNull
    private final Optional<Map<String, String>> matchTags;

    /**
     * Only include time series which has the following tags.
     */
    @NonNull
    private final Optional<Set<String>> hasTags;

    /**
     * A general set of filters. If this is combined with the other mechanisms, all the filters will
     * be AND:ed together.
     */
    @NonNull
    private final Optional<Filter> filter;

    /**
     * The date range to query for.
     */
    @NonNull
    private final Optional<QueryDateRange> range;

    @NonNull
    private final Optional<Integer> limit;
}
