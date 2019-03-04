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

package com.spotify.heroic.aggregation

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.spotify.heroic.common.Duration
import com.spotify.heroic.common.TimeUtils

@JsonInclude(JsonInclude.Include.NON_NULL)
data class SamplingQuery(var size: Duration?, var extent: Duration?) {

    @JsonCreator
    constructor(unit: String, @JsonProperty("value") size: Duration?, extent: Duration?) :
        this(size, extent ?: size)
    {
        val u = TimeUtils.parseTimeUnit(unit)

        // XXX: prefer proper durations over unit override.
        if (u.isPresent) {
            this.size = size?.withUnit(u.get())
            this.extent = extent?.withUnit(u.get())
        }
    }
}
