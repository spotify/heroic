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

import java.util.concurrent.TimeUnit

/**
 * int's are represented internally as longs.
 *
 * @author udoprog
 */
data class IntegerExpression(@JvmField val context: Context, val value: Long): Expression {
    override fun getContext() = context

    fun getValueAsInteger(): Int {
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw context.error("cannot be converted to integer")
        }

        return value.toInt()
    }

    override fun <R : Any?> visit(visitor: Expression.Visitor<R>): R {
        return visitor.visitInteger(this)
    }

    override fun multiply(other: Expression): IntegerExpression =
        IntegerExpression(context.join(other.context),
            value * other.cast(IntegerExpression::class.java).value)

    override fun divide(other: Expression): IntegerExpression =
        IntegerExpression(context.join(other.context),
            value / other.cast(IntegerExpression::class.java).value)

    override fun sub(other: Expression): IntegerExpression =
        IntegerExpression(context.join(other.context),
            value - other.cast(IntegerExpression::class.java).value)

    override fun add(other: Expression): IntegerExpression =
        IntegerExpression(context.join(other.context),
            value + other.cast(IntegerExpression::class.java).value)

    override fun negate(): IntegerExpression = IntegerExpression(context, -value)

    @Suppress("UNCHECKED_CAST")
    override fun <T : Expression?> cast(to: Class<T>): T {
        if (to.isAssignableFrom(IntegerExpression::class.java)) {
            return this as T
        }

        if (to.isAssignableFrom(DoubleExpression::class.java)) {
            return DoubleExpression(context, value.toDouble()) as T
        }

        if (to.isAssignableFrom(DurationExpression::class.java)) {
            return DurationExpression(context, TimeUnit.MILLISECONDS, value) as T
        }

        throw context.castError(this, to)
    }

    override fun toRepr() = value.toString()
}