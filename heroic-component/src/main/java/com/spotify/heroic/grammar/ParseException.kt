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

import java.lang.RuntimeException

data class ParseException(
    val partialMessage: String?,
    override val cause: Throwable?,
    val line: Int,
    val col: Int,
    val lineEnd: Int,
    val colEnd: Int
): RuntimeException("%d:%d: %s".format(line, col, partialMessage), cause) {
    constructor(message: String?, cause: Throwable?, line: Int, col: Int):
        this(message, cause, line, col, line, col)

    override fun toString() = super.toString()

    companion object {
        private const val serialVersionUID = -7313640439644659488L
    }
}