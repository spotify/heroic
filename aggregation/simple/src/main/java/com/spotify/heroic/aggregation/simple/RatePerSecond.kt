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

package com.spotify.heroic.aggregation.simple

import com.fasterxml.jackson.annotation.JsonCreator
import com.spotify.heroic.aggregation.AggregationContext
import com.spotify.heroic.aggregation.SamplingAggregation
import com.spotify.heroic.aggregation.SamplingQuery
import com.spotify.heroic.common.Duration

data class RatePerSecond(override var size: Duration?, override var extent: Duration?)
    : SamplingAggregation {

    @JsonCreator
    constructor(sampling: SamplingQuery?, size: Duration?, extent: Duration?) :
        this(size ?: sampling?.size, extent ?: sampling?.extent)

    override fun apply(context: AggregationContext?, size: Long, extent: Long): RatePerSecondInstance {
        return RatePerSecondInstance(size, extent)
    }

    companion object {
        const val NAME = "ratePerSecond"
    }
}
