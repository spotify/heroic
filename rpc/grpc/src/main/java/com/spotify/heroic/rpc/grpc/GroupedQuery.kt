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

package com.spotify.heroic.rpc.grpc

import com.fasterxml.jackson.annotation.JsonProperty
import com.spotify.heroic.common.Grouped
import com.spotify.heroic.common.UsableGroupManager
import eu.toolchain.async.AsyncFuture
import java.util.*
import java.util.function.BiFunction

data class GroupedQuery<T>(
    @JsonProperty("group") val group: Optional<String>,
    @JsonProperty("query") val query: T
) {
    fun <G : Grouped, R> apply(manager: UsableGroupManager<G>, function: (G, T) -> R): R {
        return function(
            group.map(manager::useGroup).orElseGet(manager::useDefaultGroup),
            query
        )
    }
}
