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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hasher;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode
public class MetricGroup implements Metric {
    static final List<MetricCollection> EMPTY_GROUPS = ImmutableList.of();

    private final long timestamp;
    private final List<MetricCollection> groups;

    @JsonCreator
    public MetricGroup(
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("groups") List<MetricCollection> groups
    ) {
        this.timestamp = timestamp;
        this.groups = Optional.fromNullable(groups).or(EMPTY_GROUPS);
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void hash(final Hasher hasher) {
        hasher.putInt(MetricType.GROUP.ordinal());

        for (final MetricCollection c : groups) {
            for (final Metric m : c.getData()) {
                m.hash(hasher);
            }
        }
    }

    @Override
    public boolean valid() {
        return true;
    }
}
