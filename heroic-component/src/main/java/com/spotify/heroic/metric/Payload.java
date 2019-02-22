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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.hash.Hasher;
import java.util.Map;

@AutoValue
public abstract class Payload implements Metric {
    @JsonCreator
    public static Payload create(
        @JsonProperty("timestamp") long timestamp, @JsonProperty("state") byte[] state
    ) {
        return new AutoValue_Payload(timestamp, state);
    }

    private static final Map<String, String> EMPTY_PAYLOAD = ImmutableMap.of();

    @JsonProperty
    public abstract long timestamp();
    @JsonProperty
    public abstract byte[] state();

    public boolean valid() {
        return true;
    }

    private static final Ordering<String> KEY_ORDER = Ordering.from(String::compareTo);

    @Override
    public void hash(final Hasher hasher) {
        hasher.putInt(MetricType.CARDINALITY.ordinal());
        hasher.putLong(timestamp());
        hasher.putBytes(state());
    }

    @Override
    public long getTimestamp() {
        return timestamp();
    }
}
