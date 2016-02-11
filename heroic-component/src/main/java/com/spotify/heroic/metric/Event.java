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

package com.spotify.heroic.metric;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Comparator;
import java.util.Map;

@Data
@EqualsAndHashCode(exclude = {"valueHash"})
public class Event implements Metric {
    private static final Map<String, Object> EMPTY_PAYLOAD = ImmutableMap.of();

    final long timestamp;
    final Map<String, Object> payload;
    final int valueHash;

    public Event(long timestamp) {
        this(timestamp, EMPTY_PAYLOAD);
    }

    public Event(long timestamp, Map<String, Object> payload) {
        this.timestamp = timestamp;
        this.payload = Optional.fromNullable(payload).or(EMPTY_PAYLOAD);
        this.valueHash = calculateValueHash(this.payload);
    }

    @Override
    public int valueHash() {
        return valueHash;
    }

    public boolean valid() {
        return true;
    }

    static int calculateValueHash(Map<String, Object> payload) {
        final int prime = 31;
        int result = 1;
        result = result * prime + payload.hashCode();
        return result;
    }

    public static Comparator<Metric> comparator() {
        return comparator;
    }

    static final Comparator<Metric> comparator = new Comparator<Metric>() {
        @Override
        public int compare(Metric a, Metric b) {
            return Long.compare(a.getTimestamp(), b.getTimestamp());
        }
    };
}
