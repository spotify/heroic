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

package com.spotify.heroic

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.spotify.heroic.common.DateRange
import com.spotify.heroic.common.Duration
import com.spotify.heroic.common.TimeUtils
import java.util.concurrent.TimeUnit

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(QueryDateRange.Absolute::class, name = "absolute"),
    JsonSubTypes.Type(QueryDateRange.Relative::class, name = "relative")
)
interface QueryDateRange {
    fun buildDateRange(now: Long): DateRange
    fun toDSL(): String
    @JsonIgnore fun isEmpty(): Boolean

    data class Absolute(val start: Long, val end: Long): QueryDateRange {
        override fun buildDateRange(now: Long): DateRange = DateRange(start, end)

        override fun toDSL(): String = "($start, $end)"

        override fun isEmpty(): Boolean = end - start <= 0
    }

    data class Relative(val unit: TimeUnit, val value: Long): QueryDateRange {
        @JsonCreator
        constructor(unit: String, value: Long):
            this(TimeUtils.parseTimeUnit(unit).orElse(DEFAULT_UNIT), value)

        override fun buildDateRange(now: Long): DateRange = DateRange(start(now, value), now)

        override fun isEmpty(): Boolean = value <= 0

        override fun toDSL(): String = "($value${Duration.unitSuffix(unit)})"

        fun start(now: Long, value: Long): Long = now - TimeUnit.MILLISECONDS.convert(value, unit)

        companion object {
            @JvmField val DEFAULT_UNIT = TimeUnit.DAYS
        }
    }
}
