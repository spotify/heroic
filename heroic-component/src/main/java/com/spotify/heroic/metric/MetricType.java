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

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents which type of metric to operate on.
 * <p>
 * All metric types implement TimeData, but various sources (metadata, metrics, suggestions) can
 * behave differently depending on which source is being operated on.
 */
public enum MetricType {
    // @formatter:off
    POINT(Point.class, "points"),
    SPREAD(Spread.class, "spreads"),
    GROUP(MetricGroup.class, "groups"),
    // TODO: rename to PAYLOAD.
    CARDINALITY(Payload.class, "cardinality"),
    DISTRIBUTION_POINTS(DistributionPoint.class, "distributionPoints"),
    TDIGEST_POINT(TdigestPoint.class, "tdigestPoints");
    // @formatter:on

    private final Class<? extends Metric> type;
    private final String identifier;

    MetricType(Class<? extends Metric> type, String identifier) {
        this.type = type;
        this.identifier = identifier;
    }

    public Class<? extends Metric> type() {
        return type;
    }

    public String identifier() {
        return identifier;
    }

    static final Map<String, MetricType> mapping = ImmutableMap.copyOf(Arrays
        .stream(MetricType.values())
        .collect(Collectors.toMap(MetricType::identifier, Function.identity())));

    public static Optional<MetricType> fromIdentifier(String identifier) {
        return Optional.ofNullable(mapping.get(identifier));
    }

    public boolean isAssignableFrom(MetricType other) {
        return type.isAssignableFrom(other.type);
    }

    @Override
    public String toString() {
        return identifier;
    }
}
