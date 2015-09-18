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

import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(exclude = { "valueHash" })
public class MetricGroup implements Metric {
    static final List<MetricCollection> EMPTY_GROUPS = ImmutableList.of();

    private final long timestamp;
    private final List<MetricCollection> groups;
    @JsonIgnore
    private final int valueHash;

    @JsonCreator
    public MetricGroup(@JsonProperty("timestamp") long timestamp, @JsonProperty("groups") List<MetricCollection> groups) {
        this.timestamp = timestamp;
        this.groups = Optional.fromNullable(groups).or(EMPTY_GROUPS);
        this.valueHash = calculateValueHash(this.groups);
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int valueHash() {
        return valueHash;
    }

    @Override
    public boolean valid() {
        return true;
    }

    public static Comparator<Metric> comparator() {
        return comparator;
    }

    private static final Comparator<Metric> comparator = new Comparator<Metric>() {
        @Override
        public int compare(Metric a, Metric b) {
            return Long.compare(a.getTimestamp(), b.getTimestamp());
        }
    };

    static int calculateValueHash(List<MetricCollection> groups) {
        final int prime = 31;
        int result = 1;

        for (final MetricCollection g : groups) {
            for (final Metric d : g.getData()) {
                result = prime * result + d.valueHash();
            }
        }

        return result;
    }
}