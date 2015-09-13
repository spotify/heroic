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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.common.Series;

/**
 * Represents which type of metric to operate on.
 *
 * All metric types implement TimeData, but various sources (metadata, metrics, suggestions) can behave differently
 * depending on which source is being operated on.
 */
public enum MetricType {
    // @formatter:off
    @SuppressWarnings("unchecked")
    POINT(Point.class, "points", Point.comparator(),
            (session, tags, series, values) -> session.updatePoints(tags, series, (List<Point>) values),
            (bucket, tags, values) -> ((List<Point>)values).forEach((m) -> bucket.updatePoint(tags, m))),
    @SuppressWarnings("unchecked")
    EVENT(Event.class, "events", Event.comparator(),
            (session, tags, series, values) -> session.updateEvents(tags, series, (List<Event>) values),
            (bucket, tags, values) -> ((List<Event>)values).forEach((m) -> bucket.updateEvent(tags, m))),
    @SuppressWarnings("unchecked")
    SPREAD(Spread.class, "spreads", Spread.comparator(),
            (session, tags, series, values) -> session.updateSpreads(tags, series, (List<Spread>) values),
            (bucket, tags, values) -> ((List<Spread>)values).forEach((m) -> bucket.updateSpread(tags, m))),
    @SuppressWarnings("unchecked")
    GROUP(MetricGroup.class, "groups", MetricGroup.comparator(),
            (session, tags, series, values) -> session.updateGroup(tags, series, (List<MetricGroup>) values),
            (bucket, tags, values) -> ((List<MetricGroup>)values).forEach((m) -> bucket.updateGroup(tags, m)));
    // @formatter:on

    private final Class<? extends Metric> type;
    private final String identifier;
    private final Comparator<Metric> comparator;
    private final ApplyAggregation applyAggregation;
    private final ApplyBucket applyBucket;

    private MetricType(Class<? extends Metric> type, String identifier, Comparator<Metric> comparator,
            ApplyAggregation applyAggregation, ApplyBucket applyBucket) {
        this.type = type;
        this.identifier = identifier;
        this.comparator = comparator;
        this.applyAggregation = applyAggregation;
        this.applyBucket = applyBucket;
    }

    public Class<? extends Metric> type() {
        return type;
    }

    public String identifier() {
        return identifier;
    }

    static final Map<String, MetricType> mapping = ImmutableMap.copyOf(
            Arrays.stream(MetricType.values()).collect(Collectors.toMap((MetricType m) -> m.identifier(), (m) -> m)));

    public static MetricType fromIdentifier(String identifier) {
        return mapping.get(identifier);
    }

    public boolean isAssignableFrom(MetricType other) {
        return type.isAssignableFrom(other.type);
    }

    public Comparator<Metric> comparator() {
        return comparator;
    }

    public void updateAggregation(final AggregationSession session, final Map<String, String> group,
            final Set<Series> series, final List<? extends Metric> values) {
        applyAggregation.update(session, group, series, values);
    }

    public void updateBucket(final Bucket bucket, final Map<String, String> tags, final List<? extends Metric> values) {
        applyBucket.update(bucket, tags, values);
    }

    private static interface ApplyAggregation {
        public void update(AggregationSession session, Map<String, String> tags, Set<Series> series,
                List<? extends Metric> values);
    }

    private static interface ApplyBucket {
        public void update(Bucket bucket, Map<String, String> tags, List<? extends Metric> m);
    }
}