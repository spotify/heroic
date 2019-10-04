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

package com.spotify.heroic.grammar

import java.time.Instant
import java.util.concurrent.TimeUnit

data class InstantExpression(@JvmField val context: Context, val instant: Instant): Expression {
    override fun getContext() = context

    override fun add(other: Expression): Expression {
        val o = other.cast(InstantExpression::class.java).instant.toEpochMilli()
        val m = instant.toEpochMilli() + o
        return InstantExpression(context.join(other.context), Instant.ofEpochMilli(m))
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Expression?> cast(to: Class<T>): T {
        if (to.isAssignableFrom(InstantExpression::class.java)) {
            return this as T
        }

        if (to.isAssignableFrom(IntegerExpression::class.java)) {
            return IntegerExpression(context, instant.toEpochMilli()) as T
        }

        if (to.isAssignableFrom(DoubleExpression::class.java)) {
            return DoubleExpression(context, instant.toEpochMilli().toDouble()) as T
        }

        if (to.isAssignableFrom(DurationExpression::class.java)) {
            return DurationExpression(context, TimeUnit.MILLISECONDS, instant.toEpochMilli()) as T
        }

        throw context.castError(this, to)
    }

    override fun <R : Any?> visit(visitor: Expression.Visitor<R>): R {
        return visitor.visitInstant(this)
    }

    override fun toRepr() = "{$instant}"
}
