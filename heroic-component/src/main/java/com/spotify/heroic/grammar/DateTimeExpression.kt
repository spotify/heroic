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

import java.lang.IllegalArgumentException
import java.time.DateTimeException
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.time.temporal.TemporalAccessor

data class DateTimeExpression(
    @JvmField val context: Context, 
    val dateTime: LocalDateTime
): Expression {
    override fun getContext() = context

    override fun <R : Any?> visit(visitor: Expression.Visitor<R>): R {
        return visitor.visitDateTime(this)
    }

    // TODO: support other time-zones fetched from the scope.
    override fun eval(scope: Expression.Scope): Expression {
        return InstantExpression(context, dateTime.toInstant(ZoneOffset.UTC))
    }
    
    override fun toRepr() = "{${STRING_FORMAT.format(dateTime)}}"
    
    companion object {
        private val STRING_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        private val FORMATTERS = listOf(
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        ) 
        
        @JvmStatic
        fun parse(c: Context, input: String): DateTimeExpression =
            DateTimeExpression(c, parseLocalDateTime(input))
        
        @JvmStatic
        fun parseLocalDateTime(input: String): LocalDateTime {
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
                val year = accessor.get(ChronoField.YEAR)
                val month = accessor.get(ChronoField.MONTH_OF_YEAR)
                val dayOfMonth = accessor.get(ChronoField.DAY_OF_MONTH)

                val hour = getOrDefault(accessor, ChronoField.HOUR_OF_DAY)
                val minute = getOrDefault(accessor, ChronoField.MINUTE_OF_HOUR)
                val second = getOrDefault(accessor, ChronoField.SECOND_OF_MINUTE)
                val nanoOfSecond =
                    getOrDefault(accessor, ChronoField.MILLI_OF_SECOND, 0) * 1000000

                return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nanoOfSecond)
            } else {
                val e = IllegalArgumentException("Invalid dateTime: $input")
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