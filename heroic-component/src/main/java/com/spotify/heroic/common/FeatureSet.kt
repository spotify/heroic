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

package com.spotify.heroic.common

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue

/**
 * A set of enabled and disabled features.
 */
data class FeatureSet(
    val enabled: Set<Feature> = emptySet(),
    val disabled: Set<Feature> = emptySet()
) {
    @JsonCreator
    constructor(features: Set<String>): this(
        enabled = features.filterNot { it.startsWith("-") }
            .map(Feature::create)
            .toSet(),
        disabled = features.filter { it.startsWith("-") }
            .map { Feature.create(it.substring(1)) }
            .toSet()
    )

    @JsonValue
    fun value(): List<String> {
        val features = mutableListOf<String>()
        enabled.forEach { features.add(it.id()) }
        disabled.forEach { features.add("-" + it.id()) }
        return features
    }

    fun isEmpty(): Boolean {
        return enabled.isEmpty() && disabled.isEmpty()
    }

    /**
     * Combine this feature set with another.
     *
     * @param other Other set to combine with.
     * @return a new feature set.
     */
    fun combine(other: FeatureSet): FeatureSet {
        val enabled = mutableSetOf<Feature>()
        enabled.addAll(this.enabled)
        enabled.addAll(other.enabled)

        val disabled = mutableSetOf<Feature>()
        disabled.addAll(this.disabled)
        disabled.addAll(other.disabled)

        return FeatureSet(enabled, disabled)
    }

    companion object {
        /**
         * Create an empty feature set.
         *
         * @return a new feature set.
         */
        @JvmStatic
        fun empty() = FeatureSet()

        /**
         * Create a new feature set with the given features enabled.
         *
         * @param features Features to enable in the new set.
         * @return a new feature set.
         */
        @JvmStatic
        fun of(vararg features: Feature) = FeatureSet(enabled = features.toSet())
    }
}
