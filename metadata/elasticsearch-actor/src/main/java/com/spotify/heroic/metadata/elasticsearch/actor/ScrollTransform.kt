/*
 * Copyright (c) 2019 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"): you may not use this file except in compliance
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

package com.spotify.heroic.metadata.elasticsearch.actor

import com.spotify.heroic.common.OptionalLimit
import com.spotify.heroic.elasticsearch.LimitedSet
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit

class ScrollTransform<T>(
    private val limit: OptionalLimit,
    private val converter: (SearchHit) -> T,
    private val scrollFactory: (String) -> SearchResponse
) {
    private val results = mutableSetOf<T>()

    fun <T> transform(response: SearchResponse): LimitedSet<T> {
        val hits = response.hits.hits

        hits.asSequence()
            .map(converter)
            .filter { results.add(it) }
            .takeWhile { limit.isGreaterOrEqual(results.size.toLong()) }

        @Suppress("UNCHECKED_CAST")
        results as Set<T>
        return when {
            limit.isGreater(results.size.toLong()) -> LimitedSet(limit.limitSet(results), true)
            hits.isEmpty() -> LimitedSet(results, false)
            response.scrollId == null -> LimitedSet.of()
            else ->
                // Fetch the next page in the scrolling search.
                transform(scrollFactory(response.scrollId))
        }
    }
}
