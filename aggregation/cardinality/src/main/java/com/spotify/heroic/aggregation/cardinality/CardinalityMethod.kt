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

package com.spotify.heroic.aggregation.cardinality

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.spotify.heroic.ObjectHasher
import com.spotify.heroic.grammar.*

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(CardinalityMethod.HyperLogLogCardinalityMethod::class),
    JsonSubTypes.Type(CardinalityMethod.ExactCardinalityMethod::class)
)
interface CardinalityMethod {
    fun build(timestamp: Long): CardinalityBucket

    fun reducer(): CardinalityMethod {
        throw RuntimeException("reducer not supported")
    }

    fun hashTo(hasher: ObjectHasher)

    @JsonTypeName("exact")
    data class ExactCardinalityMethod(var includeKey: Boolean) : CardinalityMethod {

        override fun build(timestamp: Long): CardinalityBucket {
            return ExactCardinalityBucket(timestamp, includeKey)
        }

        override fun hashTo(hasher: ObjectHasher) {
            hasher.putObject(javaClass,
                { hasher.putField("includeKey", includeKey, hasher.bool()) })
        }

        companion object {
            operator fun invoke(includeKey: Boolean? = null) =
                ExactCardinalityMethod(includeKey ?: false)
        }
    }

    @JsonTypeName("hll")
    data class HyperLogLogCardinalityMethod(
        val precision: Double, val includeKey: Boolean
    ) : CardinalityMethod {

        override fun build(timestamp: Long): CardinalityBucket {
            return HyperLogLogCardinalityBucket(timestamp, includeKey, precision)
        }

        override fun reducer(): CardinalityMethod {
            return ReduceHyperLogLogCardinalityMethod
        }

        override fun hashTo(hasher: ObjectHasher) {
            hasher.putObject(this.javaClass) {
                hasher.putField("precision", precision, hasher.doubleValue())
                hasher.putField("includeKey", includeKey, hasher.bool())
            }
        }

        companion object {
            operator fun invoke(precision: Double?, includeKey: Boolean?) =
                HyperLogLogCardinalityMethod(precision ?: 0.01, includeKey ?: false)
        }
    }

    @JsonTypeName("hllp")
    data class HyperLogLogPlusCardinalityMethod(
        val precision: Int, val includeKey: Boolean
    ) : CardinalityMethod {

        override fun build(timestamp: Long): CardinalityBucket {
            return HyperLogLogPlusCardinalityBucket(timestamp, includeKey, precision)
        }

        override fun reducer(): CardinalityMethod {
            return ReduceHyperLogLogCardinalityMethod
        }

        override fun hashTo(hasher: ObjectHasher) {
            hasher.putObject(this.javaClass) {
                hasher.putField("precision", precision, hasher.integer())
                hasher.putField("includeKey", includeKey, hasher.bool())
            }
        }

        companion object {
            operator fun invoke(precision: Int?, includeKey: Boolean?) =
                HyperLogLogPlusCardinalityMethod(precision ?: 16, includeKey ?: false)
        }
    }

    object ReduceHyperLogLogCardinalityMethod : CardinalityMethod {
        override fun build(timestamp: Long): CardinalityBucket {
            return ReduceHyperLogLogCardinalityBucket(timestamp)
        }

        override fun hashTo(hasher: ObjectHasher) {
            hasher.putObject(javaClass)
        }
    }

    object ReduceHyperLogLogPlusCardinalityMethod : CardinalityMethod {
        override fun build(timestamp: Long): CardinalityBucket {
            return ReduceHyperLogLogPlusCardinalityBucket(timestamp)
        }

        override fun hashTo(hasher: ObjectHasher) {
            hasher.putObject(javaClass)
        }
    }

    companion object {

        fun fromExpression(expression: Expression): CardinalityMethod {
            return expression.visit(object : Expression.Visitor<CardinalityMethod> {
                override fun visitString(e: StringExpression): CardinalityMethod {
                    return visitFunction(e.cast(FunctionExpression::class.java))
                }

                override fun visitFunction(e: FunctionExpression): CardinalityMethod {
                    when (e.name) {
                        "exact" -> return buildExact(e)
                        "hll" -> return buildHyperLogLog(e)
                        "hllp" -> return buildHyperLogLogPlus(e)
                        else -> throw e.context.error("Unknown cardinality method")
                    }
                }

                override fun defaultAction(e: Expression): CardinalityMethod {
                    throw e.context.error("Unsupported method")
                }

                private fun buildExact(e: FunctionExpression): CardinalityMethod {
                    val includeKey = e
                        .keyword("includeKey")
                        .map { i -> "true" == i.cast(StringExpression::class.java).string }
                        .orElse(null)

                    return ExactCardinalityMethod(includeKey)
                }

                private fun buildHyperLogLog(e: FunctionExpression): CardinalityMethod {
                    val precision = e
                        .keyword("precision")
                        .map { i -> i.cast(DoubleExpression::class.java).value }
                        .orElse(null)

                    val includeKey = e
                        .keyword("includeKey")
                        .map { i -> "true" == i.cast(StringExpression::class.java).string }
                        .orElse(null)

                    return HyperLogLogCardinalityMethod(precision, includeKey)
                }

                private fun buildHyperLogLogPlus(e: FunctionExpression): CardinalityMethod {
                    val precision = e
                        .keyword("precision")
                        .map { i -> i.cast(IntegerExpression::class.java).valueAsInteger }
                        .orElse(null)

                    val includeKey = e
                        .keyword("includeKey")
                        .map { i -> "true" == i.cast(StringExpression::class.java).string }
                        .orElse(null)

                    return HyperLogLogPlusCardinalityMethod(precision, includeKey)
                }
            })
        }
    }
}
