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

import com.spotify.heroic.aggregation.AggregationArguments
import java.util.*

data class FunctionExpression(
    @JvmField val context: Context,
    val name: String,
    val arguments: List<Expression>,
    val keywords: Map<String, Expression>
): Expression {
    override fun getContext() = context

    override fun eval(scope: Expression.Scope): Expression =
        FunctionExpression(context, name,
            Expression.evalList(arguments, scope), Expression.evalMap(keywords, scope))

    override fun <R : Any?> visit(visitor: Expression.Visitor<R>): R {
        return visitor.visitFunction(this)
    }

    override fun toRepr(): String {
        val a = arguments.asSequence().map(Expression::toRepr)
        val k = keywords.asSequence().map { "${it.key}=${it.value.toRepr()}" }
        val res = (a + k).joinToString(", ")

        return "$name($res)"
    }

    fun arguments() = AggregationArguments(arguments, keywords)

    fun keyword(key: String) = Optional.ofNullable(keywords[key])
}
