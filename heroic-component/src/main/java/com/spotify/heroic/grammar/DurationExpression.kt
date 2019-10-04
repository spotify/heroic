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

package com.spotify.heroic.grammar

import com.spotify.heroic.common.Duration
import java.lang.IllegalArgumentException
import java.util.concurrent.TimeUnit

data class DurationExpression(
    @JvmField val context: Context,
    val unit: TimeUnit,
    val value: Long
): Expression {
    override fun getContext() = context

    override fun <R : Any?> visit(visitor: Expression.Visitor<R>): R {
        return visitor.visitDuration(this)
    }

    override fun sub(other: Expression): DurationExpression = operate(Long::minus, other)

    override fun add(other: Expression): DurationExpression = operate(Long::plus, other)

    override fun divide(other: Expression): DurationExpression {
        val den = other.cast(IntegerExpression::class.java).value
        var value = this.value
        var unit = this.unit
        val zero = 0.toLong()

        while (value % den != zero) {
            if (unit == TimeUnit.MILLISECONDS) break

            val next = nextSmallerUnit(unit)
            value = next.convert(value, unit)
            unit = next
        }

        return DurationExpression(context, unit, value / den)
    }

    override fun negate() = DurationExpression(context, unit, -value)

    @Suppress("UNCHECKED_CAST")
    override fun <T : Expression?> cast(to: Class<T>): T {
        if (to.isAssignableFrom(DurationExpression::class.java)) {
            return this as T
        }

        if (to.isAssignableFrom(IntegerExpression::class.java)) {
            return IntegerExpression(context, toMilliseconds()) as T
        }

        throw context.castError(this, to)
    }

    override fun toRepr() = "<%d${Duration.unitSuffix(unit)}>".format(value)

    private fun nextSmallerUnit(unit: TimeUnit): TimeUnit {
        return when (unit) {
            TimeUnit.DAYS -> TimeUnit.HOURS
            TimeUnit.HOURS -> TimeUnit.MINUTES
            TimeUnit.MINUTES -> TimeUnit.SECONDS
            TimeUnit.SECONDS -> TimeUnit.MILLISECONDS
            else -> throw IllegalArgumentException("No supported smaller unit: $unit")
        }
    }

    private fun operate(op: (Long, Long) -> Long, other: Expression): DurationExpression {
        val o = other.cast(DurationExpression::class.java)
        val c = context.join(other.context)

        if (unit == o.unit) {
            return DurationExpression(c, unit, op(value, o.value))
        }

        // decide which unit to convert to depending on which has the greatest magnitude in
        // milliseconds.
        if (unit.toMillis(1) < o.unit.toMillis(1)) {
            return DurationExpression(c, unit, op(value, unit.convert(o.value, o.unit)))
        }

        return DurationExpression(c, o.unit, op(o.unit.convert(value, unit), o.value))
    }

    fun toDuration() = Duration(value, unit)

    fun toMilliseconds() = TimeUnit.MILLISECONDS.convert(value, unit)
}