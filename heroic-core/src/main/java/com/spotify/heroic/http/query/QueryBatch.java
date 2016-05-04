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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.QueryDateRange;
import lombok.Data;

import java.util.Map;
import java.util.Optional;

@Data
public class QueryBatch {
    final Map<String, QueryMetrics> queries;
    final Optional<QueryDateRange> range;

    @JsonCreator
    public QueryBatch(
        @JsonProperty("queries") Map<String, QueryMetrics> queries,
        @JsonProperty("range") QueryDateRange range
    ) {
        this.queries = Optional.ofNullable(queries).orElseGet(ImmutableMap::of);
        this.range = Optional.ofNullable(range);
    }
}
