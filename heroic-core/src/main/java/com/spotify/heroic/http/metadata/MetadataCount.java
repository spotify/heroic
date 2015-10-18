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

import static java.util.Optional.ofNullable;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.impl.TrueFilterImpl;
import com.spotify.heroic.http.query.QueryDateRange;

import lombok.Data;

@Data
public class MetadataCount {
    private static final Filter DEFAULT_FILTER = TrueFilterImpl.get();
    private static final QueryDateRange DEFAULT_DATE_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);

    private final Filter filter;
    private final DateRange range;

    @JsonCreator
    public MetadataCount(@JsonProperty("filter") Filter filter,
            @JsonProperty("range") QueryDateRange range) {
        this.filter = ofNullable(filter).orElse(DEFAULT_FILTER);
        this.range = ofNullable(range).orElse(DEFAULT_DATE_RANGE).buildDateRange();
    }

    public static MetadataCount createDefault() {
        return new MetadataCount(null, null);
    }
}