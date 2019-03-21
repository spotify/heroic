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

import com.fasterxml.jackson.annotation.JsonInclude
import com.google.common.base.Joiner
import com.spotify.heroic.common.Duration
import java.util.concurrent.TimeUnit

@JsonInclude(JsonInclude.Include.NON_NULL)
interface SamplingAggregation : Aggregation {
    val extent: Duration?
    val size: Duration?

    override fun apply(context: AggregationContext): AggregationInstance {
        val s: Duration = size ?: context.size().orElseGet { context.defaultSize() }
        val e: Duration = extent ?: size ?: context.extent().orElse(s)
        return apply(context, s.convert(TimeUnit.MILLISECONDS), e.convert(TimeUnit.MILLISECONDS))
    }

    fun apply(context: AggregationContext?, size: Long, extent: Long): AggregationInstance

    companion object {
        @JvmField val params = Joiner.on(", ")
    }
}
