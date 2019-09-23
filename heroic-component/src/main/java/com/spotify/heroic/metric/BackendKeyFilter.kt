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

package com.spotify.heroic.metric

import com.spotify.heroic.common.OptionalLimit
import java.util.*

data class BackendKeyFilter(
    val start: Optional<Start> = Optional.empty(),
    val end: Optional<End> = Optional.empty(),
    val limit: OptionalLimit = OptionalLimit.empty()
) {
    fun withStart(start: Start): BackendKeyFilter = BackendKeyFilter(Optional.of(start), end, limit)
    fun withEnd(end: End): BackendKeyFilter = BackendKeyFilter(start, Optional.of(end), limit)
    fun withLimit(limit: OptionalLimit): BackendKeyFilter = BackendKeyFilter(start, end, limit)

    companion object {


        /**
     * Match keys strictly greater than the given key.
     *
     * @param key Key to match against.
     * @return A {@link BackendKeyFilter} matching if a key is strictly greater than the given key.
     */
        @JvmStatic fun gt(key: BackendKey): GT = GT(key)
    }

    interface Start
    interface End

    /**
     * Match keys strictly greater than the given key.
     *
     * @param key Key to match against.
     * @return A {@link BackendKeyFilter} matching if a key is strictly greater than the given key.
     */
    data class GT(val key: BackendKey): Start

    /**
     * Match keys greater or equal to the given key.
     *
     * @param key The key to match against.
     * @return A {@link BackendKeyFilter} matching if a key is greater or equal to the given key.
     */
    data class GTE(val key: BackendKey): Start

    /**
     * Match keys larger or equal to the given percentage.
     *
     * @param percentage The percentage to match against, should be a value between {@code [0, 1]}.
     * @return A {@link BackendKeyFilter} matching if a key is larger or equal to the given
     * percentage.
     */
    data class GTEPercentage(val percentage: Float): Start

    /**
     * Match keys larger or equal to the given token.
     *
     * @param token The token to match against.
     * @return A {@link BackendKeyFilter} matching if a key is larger or equal to the given token.
     */
    data class GTEToken(val token: Long): Start



    /**
     * Match keys strictly smaller than the given key.
     *
     * @param key Key to match against.
     * @return A {@link BackendKeyFilter} matching if a key is strictly smaller than the given key.
     */
    data class LT(val key: BackendKey): End

    /**
     * Match keys smaller than the given percentage.
     *
     * @param percentage The percentage to match against, should be a value between {@code [0, 1]}
     * @return A {@link BackendKeyFilter} matching if a key is smaller than a given percentage.
     */
    data class LTPercentage(val percentage: Float): End

    /**
     * Match keys smaller than the given token.
     *
     * @param token The token to match against.
     * @return A {@link BackendKeyFilter} matching if a key is smaller than the given token.
     */
    data class LTToken(val token: Long): End
}