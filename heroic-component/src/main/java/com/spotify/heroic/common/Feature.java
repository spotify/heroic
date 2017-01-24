/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.heroic.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Features that can be enabled in configuration, or per-query.
 *
 * Features should be formulated so that they are enabled when they are present, not the other way
 * around which would be something like {@code DISABLE_SHIFT_RANGE}.
 * Supporting such duality for every feature would cause more code and add to confusion.
 */
public enum Feature {
    /**
     * Enable feature to only perform aggregations that can be performed with limited resources.
     * <p>
     * Aggregations are commonly performed per-shard, and the result concatenated. This enabled
     * experimental support for distributed aggregations which behave transparently across shards.
     */
    DETERMINISTIC_AGGREGATIONS("com.spotify.heroic.deterministic_aggregations"),
    /**
     * Enable feature to perform distributed aggregations.
     * <p>
     * Aggregations are commonly performed per-shard, and the result concatenated. This enabled
     * experimental support for distributed aggregations which behave transparently across shards.
     */
    DISTRIBUTED_AGGREGATIONS("com.spotify.heroic.distributed_aggregations"),

    /**
     * Enable feature to cause range to be rounded on the current cadence.
     * <p>
     * This will assert that there are data outside of the range queried for, which is a useful
     * feature when using a dashboarding system.
     */
    SHIFT_RANGE("com.spotify.heroic.shift_range"),

    /**
     * Enable feature to cause data to be fetched in slices.
     */
    SLICED_DATA_FETCH("com.spotify.heroic.sliced_data_fetch");

    private final String id;

    Feature(final String id) {
        this.id = id;
    }

    @JsonCreator
    public static Feature create(final String id) {
        for (final Feature f : values()) {
            if (f.id.equals(id)) {
                return f;
            }
        }

        throw new IllegalArgumentException(id);
    }

    @JsonValue
    public String id() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }
}
