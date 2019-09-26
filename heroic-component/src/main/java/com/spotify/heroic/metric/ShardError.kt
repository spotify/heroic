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

package com.spotify.heroic.metric

import com.spotify.heroic.cluster.ClusterShard

data class ShardError(
    val nodes: List<String>,
    val shard: Map<String, String>,
    val error: String
): RequestError {
    companion object {
        @JvmStatic
        fun fromThrowable(c: ClusterShard, e: Throwable) =
            ShardError(c.nodesAsStringList, c.shard, errorMessage(e))

        @JvmStatic
        fun errorMessage(e: Throwable): String {
            val message = e.message ?: "<null>"

            val cause: Throwable = e.cause ?: return message

            return "$message, caused by ${errorMessage(cause)}"
        }
    }
}