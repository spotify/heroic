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

package com.spotify.heroic.shell.task

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

data class TestOutput(
    val context: String,
    val concurrency: Int,
    val errors: Int,
    val mismatches: Int,
    val matches: Int,
    val count: Int,
    val times: List<Long>
)

data class TestPartialResult(
    val times: List<Long>,
    val errors: Int,
    val mismatches: Int,
    val matches: Int
)

data class TestResult(
    val context: String,
    val concurrency: Int,
    val times: List<Long>,
    val errors: Int,
    val mismatches: Int,
    val matches: Int,
    val count: Int
)

data class TestSuite(
    @JsonProperty("concurrenty") val concurrency: MutableList<Int>,
    val tests: List<TestCase>
)

data class TestCase(
    val context: String,
    val count: Int,
    val suggestions: List<TestSuggestion>
)

data class TestSuggestion(
    val input: Suggestion,
    val expect: Set<Suggestion>
)

data class Suggestion(
    val key: String?,
    val value: String?
) {
    val optionalKey: Optional<String>
        get() = Optional.ofNullable(key)

    val optionalValue: Optional<String>
        get() = Optional.ofNullable(value)
}