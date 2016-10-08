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

import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.filter.Filter;
import lombok.Data;
import lombok.NonNull;

import java.util.List;
import java.util.Optional;

@Data
public class MetadataTagValuesSuggest {
    public static final long DEFAULT_LIMIT = 10;
    public static final long DEFAULT_GROUP_LIMIT = 10;

    /**
     * Filter the suggestions being returned.
     */
    @NonNull
    private final Optional<Filter> filter;

    /**
     * Limit the number of suggestions being returned.
     */
    @NonNull
    private final Optional<Long> limit;

    /**
     * Query for tags within the given range.
     */
    @NonNull
    private final Optional<QueryDateRange> range;

    /**
     * Exclude the given tags from the result.
     */
    @NonNull
    private final Optional<List<String>> exclude;

    /**
     * Limit the number of values a single suggestion group may contain.
     */
    @NonNull
    private final Optional<Long> groupLimit;
}
