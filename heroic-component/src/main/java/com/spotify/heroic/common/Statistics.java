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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Data;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class Statistics {
    static final Statistics EMPTY = new Statistics(ImmutableMap.of());

    private final Map<String, Long> counters;

    @JsonCreator
    public Statistics(@JsonProperty("counters") Map<String, Long> counters) {
        this.counters = checkNotNull(counters, "counters");
    }

    public Statistics merge(Statistics other) {
        final ImmutableMap.Builder<String, Long> counters = ImmutableMap.builder();
        final Set<String> keys = ImmutableSet.<String>builder()
            .addAll(this.counters.keySet())
            .addAll(other.counters.keySet())
            .build();

        for (final String k : keys) {
            counters.put(k, this.counters.getOrDefault(k, 0L) + other.counters.getOrDefault(k, 0L));
        }

        return new Statistics(counters.build());
    }

    public static Statistics of(Map<String, Long> samples) {
        return new Statistics(ImmutableMap.copyOf(samples));
    }

    public static Statistics of(String k1, long v1) {
        return new Statistics(ImmutableMap.of(k1, v1));
    }

    public static Statistics of(String k1, long v1, String k2, long v2) {
        return new Statistics(ImmutableMap.of(k1, v1, k2, v2));
    }

    public static Statistics of(String k1, long v1, String k2, long v2, String k3, long v3) {
        return new Statistics(ImmutableMap.of(k1, v1, k2, v2, k3, v3));
    }

    public static Statistics of(
        String k1, long v1, String k2, long v2, String k3, long v3, String k4, long v4
    ) {
        return new Statistics(ImmutableMap.of(k1, v1, k2, v2, k3, v3, k4, v4));
    }

    public long get(final String key, final long defaultValue) {
        return counters.getOrDefault(key, defaultValue);
    }

    public static Statistics empty() {
        return EMPTY;
    }
}
