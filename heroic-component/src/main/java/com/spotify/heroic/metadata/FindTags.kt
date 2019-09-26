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

package com.spotify.heroic.metadata

import com.spotify.heroic.cluster.ClusterShard
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.OptionalLimit
import com.spotify.heroic.filter.Filter
import com.spotify.heroic.metric.RequestError
import com.spotify.heroic.metric.ShardError
import eu.toolchain.async.Collector
import eu.toolchain.async.Transform

data class FindTags(
    val errors: List<RequestError> = listOf(),
    val tags: Map<String, Set<String>> = mapOf(),
    val size: Int = 0
) {
    constructor(tags: Map<String, Set<String>>, size: Int): this(listOf(), tags, size)

    companion object {
        @JvmField val EMPTY = FindTags()

        @JvmStatic fun reduce(): Collector<FindTags, FindTags> {
            return Collector { results: Collection<FindTags> ->
                val errors = mutableListOf<RequestError>()
                val tags = mutableMapOf<String, MutableSet<String>>()
                var size = 0

                results.forEach {
                    errors.addAll(it.errors)
                    it.tags.forEach { (key, value) ->
                        tags.getOrDefault(key, mutableSetOf()).addAll(value)
                    }
                    size += it.size
                }

                FindTags(errors, tags, size)
            }
        }

        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, FindTags> {
            return Transform { e: Throwable ->
                FindTags(errors = listOf(ShardError.fromThrowable(shard, e)))
            }
        }
    }

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit
    )
}

