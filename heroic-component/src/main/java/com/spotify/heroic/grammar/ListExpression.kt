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

data class ListExpression(@JvmField val context: Context, val list: List<Expression>): Expression {
    override fun getContext() = context

    override fun eval(scope: Expression.Scope): Expression {
        val list = Expression.evalList(list, scope)
        return ListExpression(context, list)
    }

    override fun <R : Any?> visit(visitor: Expression.Visitor<R>): R {
        return visitor.visitList(this)
    }

    override fun add(other: Expression): ListExpression {
        val o: ListExpression = other.cast(ListExpression::class.java)
        val list = listOf(*this.list.toTypedArray(), *o.list.toTypedArray())

        return ListExpression(context.join(other.context), list)
    }

    override fun toRepr() = list.joinToString(", ", transform = Expression::toRepr)
}
