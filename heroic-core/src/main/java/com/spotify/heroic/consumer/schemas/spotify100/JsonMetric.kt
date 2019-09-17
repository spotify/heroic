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

package com.spotify.heroic.consumer.schemas.spotify100

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class JsonMetric(
    val key: String?,
    @Deprecated("Host should be set as a normal tag")
    val host: String?,
    val time: Long?,
    @JsonProperty("attributes", access = JsonProperty.Access.WRITE_ONLY)
    val rawAttributes: Map<String, String?> = emptyMap(),
    @JsonProperty("resource", access = JsonProperty.Access.WRITE_ONLY)
    val rawResource: Map<String, String?>?,
    val value: Double?
) {
    @Suppress("UNCHECKED_CAST")
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    val attributes = rawAttributes.filter { it.value != null } as Map<String, String>

    @Suppress("UNCHECKED_CAST")
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    val resource =
        (rawResource ?: emptyMap()).filter { it.value != null } as Map<String, String>
}

