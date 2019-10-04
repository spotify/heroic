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

import java.lang.IllegalArgumentException

data class Groups(val groups: Set<String> = emptySet()): Iterable<String> {
    constructor(group: String): this(setOf(group))

    fun or(vararg defaultGroups: String): Groups {
        if (defaultGroups.isEmpty()) {
            throw IllegalArgumentException("Set of default groups must not be empty")
        }

        if (groups.isNotEmpty()) return this

        return Groups(defaultGroups.toSet())
    }

    override fun iterator(): Iterator<String> = groups.iterator()

    fun isEmpty() = groups.isEmpty()

    companion object {
        @JvmStatic
        fun empty() = Groups()

        @JvmStatic
        fun combine(first: Groups, vararg other: Groups): Groups {
            val set = mutableSetOf<String>()
            set.addAll(first.groups)
            other.flatMapTo(set, { it.groups })
            return Groups(set.toSet())
        }
    }
}
