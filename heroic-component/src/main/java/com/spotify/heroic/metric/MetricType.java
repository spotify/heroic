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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

/**
 * Represents which type of metric to operate on.
 *
 * All metric types implement TimeData, but various sources (metadata, metrics, suggestions) can
 * behave differently depending on which source is being operated on.
 */
public enum MetricType {
    // @formatter:off
    POINT(Point.class, "points", Point.comparator()),
    EVENT(Event.class, "events", Event.comparator()),
    SPREAD(Spread.class, "spreads", Spread.comparator()),
    GROUP(MetricGroup.class, "groups", MetricGroup.comparator());
    // @formatter:on

    private final Class<? extends Metric> type;
    private final String identifier;
    private final Comparator<Metric> comparator;

    private MetricType(Class<? extends Metric> type, String identifier,
            Comparator<Metric> comparator) {
        this.type = type;
        this.identifier = identifier;
        this.comparator = comparator;
    }

    public Class<? extends Metric> type() {
        return type;
    }

    public String identifier() {
        return identifier;
    }

    static final Map<String, MetricType> mapping =
            ImmutableMap.copyOf(Arrays.stream(MetricType.values())
                    .collect(Collectors.toMap((MetricType m) -> m.identifier(), (m) -> m)));

    public static Optional<MetricType> fromIdentifier(String identifier) {
        return Optional.ofNullable(mapping.get(identifier));
    }

    public boolean isAssignableFrom(MetricType other) {
        return type.isAssignableFrom(other.type);
    }

    public Comparator<Metric> comparator() {
        return comparator;
    }

    @Override
    public String toString() {
        return identifier;
    }
}
