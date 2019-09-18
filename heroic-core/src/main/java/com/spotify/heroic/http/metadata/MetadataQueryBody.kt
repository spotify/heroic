/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.http.metadata

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.spotify.heroic.QueryDateRange
import com.spotify.heroic.filter.Filter
import java.util.*

@JsonIgnoreProperties(ignoreUnknown = true)
data class MetadataQueryBody(
    // Only include time series which match the exact key.
    val matchKey: Optional<String>,
    // Only include time series which matches the exact key/value combination.
    val matchTags: Optional<Map<String, String>>,
    // Only include time series which has the following tags.
    val hasTags: Optional<Set<String>>,
    // A general set of filters. If this is combined with the other mechanisms, all the filters will
    // be AND:ed together.
    val filter: Optional<Filter>,
    // The date range to query for.
    val range: Optional<QueryDateRange>,
    val limit: Optional<Int>
    ) {
    companion object {
        const val DEFAULT_LIMIT = 50
    }
}