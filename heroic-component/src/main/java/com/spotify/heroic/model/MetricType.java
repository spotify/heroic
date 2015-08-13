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

package com.spotify.heroic.model;

import java.util.Comparator;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Represents which type of metric to operate on.
 *
 * All metric types implement TimeData, but various sources (metadata, metrics, suggestions) can behave differently
 * depending on which source is being operated on.
 */
public enum MetricType {
    // @formatter:off
    POINTS(DataPoint.class, "points", "series", DataPoint.comparator()),
    EVENTS(Event.class, "events", "events", Event.comparator()),
    SPREAD(Spread.class, "spread", "spread", Spread.comparator());
    // @formatter:on

    private final Class<? extends TimeData> type;
    private final String identifier;
    private final String group;
    private final Comparator<TimeData> comparator;

    private MetricType(Class<? extends TimeData> type, String identifier, String group, Comparator<TimeData> comparator) {
        this.type = type;
        this.identifier = identifier;
        this.group = group;
        this.comparator = comparator;
    }

    public Class<? extends TimeData> type() {
        return type;
    }

    public String identifier() {
        return identifier;
    }

    public String group() {
        return group;
    }

    // @formatter:off
    private static final Map<String, MetricType> mapping = ImmutableMap.of(
        POINTS.identifier, POINTS,
        EVENTS.identifier, EVENTS
    );
    // @formatter:on

    public static MetricType fromIdentifier(String identifier) {
        return mapping.get(identifier);
    }

    public boolean isAssignableFrom(MetricType other) {
        return type.isAssignableFrom(other.type);
    }

    public Comparator<TimeData> comparator() {
        return comparator;
    }
}