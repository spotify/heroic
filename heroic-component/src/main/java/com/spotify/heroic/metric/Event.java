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

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.hash.Hasher;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Data
@EqualsAndHashCode
public class Event implements Metric {
    private static final Map<String, String> EMPTY_PAYLOAD = ImmutableMap.of();

    private final long timestamp;
    private final Map<String, String> payload;

    public Event(final long timestamp) {
        this(timestamp, EMPTY_PAYLOAD);
    }

    public Event(final long timestamp, final Map<String, String> payload) {
        this.timestamp = timestamp;
        this.payload = Optional.fromNullable(payload).or(EMPTY_PAYLOAD);
    }

    public boolean valid() {
        return true;
    }

    private static final Ordering<String> KEY_ORDER = Ordering.from(String::compareTo);

    @Override
    public void hash(final Hasher hasher) {
        hasher.putInt(MetricType.EVENT.ordinal());

        for (final String k : KEY_ORDER.sortedCopy(payload.keySet())) {
            hasher.putString(k, Charsets.UTF_8).putString(payload.get(k), Charsets.UTF_8);
        }
    }
}
