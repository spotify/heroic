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

data class DoubleExpression(@JvmField val context: Context, val value: Double): Expression {
    override fun getContext() = context

    override fun <R : Any?> visit(visitor: Expression.Visitor<R>): R {
        return visitor.visitDouble(this)
    }

    override fun multiply(other: Expression): DoubleExpression =
        DoubleExpression(context.join(other.context),
            value * other.cast(DoubleExpression::class.java).value)

    override fun divide(other: Expression): DoubleExpression =
        DoubleExpression(context.join(other.context),
            value / other.cast(DoubleExpression::class.java).value)

    override fun sub(other: Expression): DoubleExpression =
        DoubleExpression(context.join(other.context),
            value - other.cast(DoubleExpression::class.java).value)

    override fun add(other: Expression): DoubleExpression =
        DoubleExpression(context.join(other.context),
            value + other.cast(DoubleExpression::class.java).value)

    override fun negate(): DoubleExpression = DoubleExpression(context, -value)

    @Suppress("UNCHECKED_CAST")
    override fun <T : Expression?> cast(to: Class<T>): T {
        if (to.isAssignableFrom(DoubleExpression::class.java)) {
            return this as T
        }

        if (to.isAssignableFrom(IntegerExpression::class.java)) {
            return IntegerExpression(context, value.toLong()) as T
        }

        if (to.isAssignableFrom(DurationExpression::class.java)) {
            return DurationExpression(context, TimeUnit.MILLISECONDS, value.toLong()) as T
        }

        throw context.castError(this, to)
    }

    override fun toRepr() = value.toString()

    override fun toString() = "<${toRepr()} $context>"
}