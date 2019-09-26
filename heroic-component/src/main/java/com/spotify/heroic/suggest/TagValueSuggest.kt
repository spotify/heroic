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

package com.spotify.heroic.suggest

import com.spotify.heroic.cluster.ClusterShard
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.OptionalLimit
import com.spotify.heroic.filter.Filter
import com.spotify.heroic.metric.RequestError
import com.spotify.heroic.metric.ShardError
import eu.toolchain.async.Collector
import eu.toolchain.async.Transform
import java.util.*

data class TagValueSuggest(
    val errors: List<RequestError>,
    val values: List<String>,
    val limited: Boolean
) {
    constructor(values: List<String>, limited: Boolean): this(listOf(), values, limited)
    
    companion object {
        @JvmStatic fun reduce(limit: OptionalLimit): Collector<TagValueSuggest, TagValueSuggest> {
            return Collector { groups: Collection<TagValueSuggest> ->
                val errors1: MutableList<RequestError> = mutableListOf()
                val values1: SortedSet<String> = TreeSet()
                var limited1 = false

                groups.forEach {
                    errors1.addAll(it.errors)
                    values1.addAll(it.values)
                    limited1 = limited1 || it.limited
                }

                limited1 = limited1 || limit.isGreater(values1.size.toLong())

                TagValueSuggest(errors1, limit.limitList(values1.toList()), limited1)
            }
        }
        
        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, TagValueSuggest> {
            return Transform { e: Throwable ->
                TagValueSuggest(listOf(ShardError.fromThrowable(shard, e)), listOf(), false)
            }
        }
    }

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit,
        val key: Optional<String>
    )
}