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

package com.spotify.heroic.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.Filter;

import lombok.Data;

@Data
public class RangeFilter {
    private final Filter filter;
    private final DateRange range;
    private final int limit;

    @JsonCreator
    public RangeFilter(@JsonProperty("filter") Filter filter,
            @JsonProperty("range") DateRange range, @JsonProperty("limit") int limit) {
        this.filter = checkNotNull(filter);
        this.range = checkNotNull(range);
        this.limit = checkNotNull(limit);
    }

    public static RangeFilter filterFor(Filter filter, Optional<DateRange> range, final long now) {
        return new RangeFilter(filter, range.orElseGet(() -> defaultDateRange(now)),
                Integer.MAX_VALUE);
    }

    public static RangeFilter filterFor(Filter filter, Optional<DateRange> range, final long now,
            int limit) {
        return new RangeFilter(filter, range.orElseGet(() -> defaultDateRange(now)), limit);
    }

    public static DateRange defaultDateRange(final long now) {
        final long start = now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
        return new DateRange(start, now);
    }
}
