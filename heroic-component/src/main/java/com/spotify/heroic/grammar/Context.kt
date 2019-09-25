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

import com.fasterxml.jackson.annotation.JsonTypeName
import java.lang.Exception
import kotlin.math.max
import kotlin.math.min

data class Context(
    val line: Int,
    val col: Int,
    val lineEnd: Int,
    val colEnd: Int
) {
    fun error(message: String) = ParseException(message, null, line, col, lineEnd, colEnd)

    fun error(cause: Exception) = ParseException(cause.message, cause, line, col, lineEnd, colEnd)

    fun castError(from: Expression, to: Class<*>) =
        error("${from.toRepr()} cannot be cast to ${name(to)}")

    fun scopeLookupError(name: String) =
        error("cannot find reference ($name) in the current scope")

    fun join(o: Context) =
        Context(min(line, o.line), min(col, o.col), max(lineEnd, o.lineEnd), max(colEnd, o.colEnd))

    override fun toString() = "${toStringLine()}:$col-$colEnd"

    private fun toStringLine(): String {
        return if (line != lineEnd) "$line-$lineEnd"
        else line.toString()
    }

    companion object {
        @JvmStatic
        fun name(type: Class<*>): String {
            val name = type.getAnnotation(JsonTypeName::class.java)
            return name?.value ?: type.simpleName
        }
    }
}
