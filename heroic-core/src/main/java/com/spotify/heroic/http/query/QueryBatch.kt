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

package com.spotify.heroic.http.query

import com.spotify.heroic.QueryDateRange
import com.spotify.heroic.metric.Arithmetic
import com.spotify.heroic.metric.QueryMetrics
import java.util.*

/**
 * Encapsulates multiple query definitions with accompanying arithmetic query
 * definitions that pertain to those queries. For example:<p>
 * <pre>
 * queries = {  "A" → { query:..., source:..., filter:... },
 *              "B" → { query:..., source:..., filter:... }, ... }
 *
 * arithmeticQueries = { "C" → "A / B * 100" }
 * </pre><p>
 * - you can see illustrated here that "C" pertains to the queries "A" and "B"
 * in the <code>queries</code> parameter.
 *
 * NOTE that any variables referenced in <code>arithmeticQueries</code> (in this example
 * "A" and "B", must be present as keys of the `queries` parameter.
 */
data class QueryBatch(
        val queries: Optional<Map<String, QueryMetrics>>,
        val range: Optional<QueryDateRange>,
        val arithmeticQueries: Optional<Map<String, Arithmetic>>
) {
    constructor(queries: Optional<Map<String, QueryMetrics>>,
                range: Optional<QueryDateRange>) : this(queries, range, Optional.empty())
}
