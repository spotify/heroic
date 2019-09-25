/*
 * Copyright (c) 2019 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License") you may not use this file except in compliance
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

package com.spotify.heroic.grammar

import java.time.DateTimeException
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.time.temporal.TemporalAccessor

/**
 * Expression representing a given time of day, like 12:00:00.
 * <p>
 * This does <em>not</em> contain timezone or day information, and when evaluated using {@link
 * #eval(com.spotify.heroic.grammar.Expression.Scope)} is converted to an {@link
 * com.spotify.heroic.grammar.InstantExpression} using the information available in the scope.
 */
data class TimeExpression(
    @JvmField val context: Context,
    val hours: Int,
    val minutes: Int,
    val seconds: Int,
    val milliSeconds: Int
): Expression {
    override fun getContext() = context

    override fun <R : Any?> visit(visitor: Expression.Visitor<R>): R {
        return visitor.visitTime(this)
    }

    override fun eval(scope: Expression.Scope): Expression {
        val now = scope.lookup(context, Expression.NOW).cast(IntegerExpression::class.java).value
        val nowInstant = Instant.ofEpochMilli(now)
        val local = LocalDateTime.ofInstant(nowInstant, ZoneOffset.UTC)

        val year = local.get(ChronoField.YEAR)
        val month = local.get(ChronoField.MONTH_OF_YEAR)
        val dayOfMonth = local.get(ChronoField.DAY_OF_MONTH)

        val instant = LocalDateTime
            .of(year, month, dayOfMonth, hours, minutes, seconds, milliSeconds * 1000000)
            .toInstant(ZoneOffset.UTC)

        return InstantExpression(context, instant)
    }

    override fun toRepr() = "{{%02d:%02d:%02d.%03d}}".format(hours, minutes, seconds, milliSeconds)
    
    companion object {
        private val FORMATTERS = listOf(
            DateTimeFormatter.ofPattern("HH:mm"),
            DateTimeFormatter.ofPattern("HH:mm:ss"),
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
        )

        @JvmStatic
        fun parse(c: Context, input: String): TimeExpression {
            val errors = mutableListOf<Throwable>()

            val accessor = FORMATTERS.asSequence().mapNotNull {
                try {
                    it.parse(input)
                } catch (e: DateTimeException) {
                    errors.add(e)
                    null
                }
            }.firstOrNull()

            if (accessor != null) {
                val hours = accessor.get(ChronoField.HOUR_OF_DAY)
                val minutes = accessor.get(ChronoField.MINUTE_OF_HOUR)
                val seconds = getOrDefault(accessor, ChronoField.SECOND_OF_MINUTE)
                val milliSeconds = getOrDefault(accessor, ChronoField.MILLI_OF_SECOND)

                return TimeExpression(c, hours, minutes, seconds, milliSeconds)
            } else {
                val e = IllegalArgumentException("Invalid instant: $input")
                errors.forEach(e::addSuppressed)
                throw e
            }
        }

        private fun getOrDefault(
            accessor: TemporalAccessor, field: ChronoField, default: Int = 0
        ): Int {
            return if (accessor.isSupported(field)) accessor.get(field)
            else default
        }
    }
}