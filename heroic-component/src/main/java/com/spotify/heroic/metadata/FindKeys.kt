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

data class FindKeys(
    val errors: List<RequestError> = listOf(),
    val keys: Set<String> = setOf(),
    val size: Int = 0,
    val duplicates: Int = 0
) {
    constructor(keys: Set<String>, size: Int, duplicates: Int):
        this(listOf(), keys, size, duplicates)

    companion object {
        @JvmStatic fun reduce(): Collector<FindKeys, FindKeys> {
            return Collector { results: Collection<FindKeys> ->
                val errors = mutableListOf<RequestError>()
                val keys = mutableSetOf<String>()
                var size = 0
                var duplicates = 0

                results.forEach { result ->
                    errors.addAll(result.errors)
                    result.keys.filter { keys.add(it) }.forEach { _ -> duplicates += 1 }
                    duplicates += result.duplicates
                    size += result.size
                }

                FindKeys(errors, keys, size, duplicates)
            }
        }

        @JvmStatic fun shardError(shard: ClusterShard): Transform<Throwable, FindKeys> {
            return Transform { e: Throwable ->
                FindKeys(errors = listOf(ShardError.fromThrowable(shard, e)))
            }
        }
    }

    data class Request(
        val filter: Filter,
        val range: DateRange,
        val limit: OptionalLimit
    )
}

