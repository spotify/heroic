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

package com.spotify.heroic.consumer.collectd

data class CollectdSample(
    val host: String,
    val time: Long,
    val plugin: String,
    val pluginInstance: String,
    val type: String,
    val typeInstance: String,
    val values: List<CollectdValue>,
    val interval: Long,
    val message: String,
    val severity: Long
)

enum class CollectdSampleType(val value: Int) {
    COUNTER(0),
    GAUGE(1),
    DERIVE(2),
    ABSOLUTE(3);

    companion object {
        private val map = values().associateBy(CollectdSampleType::value)
        @JvmStatic fun fromValue(value: Int): CollectdSampleType? = map[value]
    }
}