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
import java.lang.IllegalStateException
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/**
 * A helper type that represents a duration as the canonical duration/unit.
 * <p>
 * This is provided so that we can implement a parser for it to simplify configurations that require
 * durations.
 * <p>
 * This type is intended to be conveniently de-serialize from a short-hand string type, like the
 * following examples.
 * <p>
 * <ul> <li>1H - 1 Hour</li> <li>5m - 5 minutes</li> </ul>
 *
 * @author udoprog
 */
data class Duration(val duration: Long, var unit: TimeUnit? = TimeUnit.SECONDS) {
    init {
        unit = unit ?: TimeUnit.SECONDS
    }

    fun convert(unit: TimeUnit) = unit.convert(duration, this.unit)

    fun toMilliseconds() = convert(TimeUnit.MILLISECONDS)

    fun withUnit(other: TimeUnit) = Duration(duration, other)

    fun toDSL() = duration.toString() + unitSuffix(unit!!)

    companion object {
        @JvmField val DEFAULT_UNIT = TimeUnit.MILLISECONDS

        @JvmStatic
        fun of(duration: Long, unit: TimeUnit?) = Duration(duration, unit)

        private val PATTERN = Pattern.compile("^(\\d+)([a-zA-Z]*)$")

        private val units = mapOf(
            "ms" to TimeUnit.MILLISECONDS,
            "s" to TimeUnit.SECONDS,
            "m" to TimeUnit.MINUTES,
            "h" to TimeUnit.HOURS,
            "H" to TimeUnit.HOURS,
            "d" to TimeUnit.DAYS
        )

        fun unitSuffix(unit: TimeUnit): String {
            return when (unit) {
                TimeUnit.MILLISECONDS -> "ms"
                TimeUnit.SECONDS -> "s"
                TimeUnit.MINUTES -> "m"
                TimeUnit.HOURS -> "h"
                TimeUnit.DAYS -> "d"
                else -> throw IllegalStateException("Unit not supported for serialization: $unit")
            }
        }

        @JvmStatic
        fun parseDuration(string: String): Duration {
            val m = PATTERN.matcher(string)

            if (!m.matches()) throw IllegalArgumentException("Invalid duration: $string")

            val duration = m.group(1).toLong()
            val unitString = m.group(2)

            if (unitString.isEmpty()) return Duration(duration, DEFAULT_UNIT)

            if ("w" == unitString) return Duration(duration * 7, TimeUnit.DAYS)

            val unit = units[unitString]
                ?: throw IllegalArgumentException("Invalid unit ($unitString) in duration: $string")

            return Duration(duration, unit)
        }
    }
}
