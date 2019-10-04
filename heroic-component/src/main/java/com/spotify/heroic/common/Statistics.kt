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

package com.spotify.heroic.common

import java.util.*

data class Statistics(val counters: Map<String, Long> = emptyMap()) {
    constructor(k1: String, v1: Long): this(mapOf(k1 to v1))

    constructor(k1: String, v1: Long, k2: String, v2: Long):
        this(mapOf(k1 to v1, k2 to v2))

    fun merge(other: Statistics): Statistics {
        val counters = mutableMapOf<String, Long>()

        val keys = mutableSetOf<String>()
        keys.addAll(this.counters.keys)
        keys.addAll(other.counters.keys)

        keys.forEach {
            val otherValue = other.counters.getOrDefault(it, 0L)
            val thisValue = this.counters.getOrDefault(it, 0L)
            counters[it] = otherValue + thisValue
        }

        return Statistics(counters.toMap())
    }

    fun getCounterValue(key: String) = Optional.ofNullable(counters[key])

    fun get(key: String, defaultValue: Long) = counters.getOrDefault(key, defaultValue)

    companion object {
        @JvmStatic fun empty() = Statistics()
    }
}
