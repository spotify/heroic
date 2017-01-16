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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.Series;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * A collection of metrics.
 * <p>
 * Metrics are constrained to the implemented types below, so far these are {@link Point}, {@link
 * Event}, {@link Spread} , and {@link MetricGroup}.
 * <p>
 * There is a JSON serialization available in {@link com.spotify.heroic.metric.MetricCollection}
 * which correctly preserves the type information of these collections.
 * <p>
 * This class is a carrier for _any_ of these metrics, the canonical way for accessing the
 * underlying data is to first check it's type using {@link #getType()}, and then access the data
 * with the appropriate cast using {@link #getDataAs(Class)}.
 * <p>
 * The following is an example for how you may access data from the collection.
 * <p>
 * <pre>
 * final MetricCollection collection = ...;
 *
 * if (collection.getType() == MetricType.POINT) {
 *     final List<Point> points = collection.getDataAs(Point.class);
 *     ...
 * }
 * </pre>
 *
 * @author udoprog
 * @see Point
 * @see Spread
 * @see Event
 * @see MetricGroup
 */
@Data
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public abstract class MetricCollection {
    final MetricType type;
    final List<? extends Metric> data;

    /**
     * Helper method to fetch a collection of the given type, if applicable.
     *
     * @param expected The expected type to read.
     * @return A list of the expected type.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getDataAs(Class<T> expected) {
        if (!expected.isAssignableFrom(type.type())) {
            throw new IllegalArgumentException(
                String.format("Cannot assign type (%s) to expected (%s)", type, expected));
        }

        return (List<T>) data;
    }

    /**
     * Update the given aggregation with the content of this collection.
     */
    public abstract void updateAggregation(
        AggregationSession session, Map<String, String> tags, Set<Series> series
    );

    public int size() {
        return data.size();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    private static final MetricCollection empty = new EmptyMetricCollection();

    public static MetricCollection empty() {
        return empty;
    }

    // @formatter:off
    private static final
    Map<MetricType, Function<List<? extends Metric>, MetricCollection>> adapters = ImmutableMap.of(
        MetricType.GROUP, GroupCollection::new,
        MetricType.POINT, PointCollection::new,
        MetricType.EVENT, EventCollection::new,
        MetricType.SPREAD, SpreadCollection::new,
        MetricType.CARDINALITY, CardinalityCollection::new
    );
    // @formatter:on

    public static MetricCollection groups(List<MetricGroup> metrics) {
        return new GroupCollection(metrics);
    }

    public static MetricCollection points(List<Point> metrics) {
        return new PointCollection(metrics);
    }

    public static MetricCollection events(List<Event> metrics) {
        return new EventCollection(metrics);
    }

    public static MetricCollection spreads(List<Spread> metrics) {
        return new SpreadCollection(metrics);
    }

    public static MetricCollection cardinality(List<Payload> metrics) {
        return new CardinalityCollection(metrics);
    }

    public static MetricCollection build(
        final MetricType key, final List<? extends Metric> metrics
    ) {
        final Function<List<? extends Metric>, MetricCollection> adapter =
            checkNotNull(adapters.get(key), "adapter does not exist for type");
        return adapter.apply(metrics);
    }

    public static MetricCollection mergeSorted(
        final MetricType type, final List<List<? extends Metric>> values
    ) {
        final List<Metric> data = ImmutableList.copyOf(Iterators.mergeSorted(
            ImmutableList.copyOf(values.stream().map(Iterable::iterator).iterator()),
            Metric.comparator()));
        return build(type, data);
    }

    @SuppressWarnings("unchecked")
    private static class PointCollection extends MetricCollection {
        PointCollection(List<? extends Metric> points) {
            super(MetricType.POINT, points);
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updatePoints(tags, series, adapt());
        }

        private List<Point> adapt() {
            return (List<Point>) data;
        }
    }

    @SuppressWarnings("unchecked")
    private static class EventCollection extends MetricCollection {
        EventCollection(List<? extends Metric> events) {
            super(MetricType.EVENT, events);
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateEvents(tags, series, adapt());
        }

        private List<Event> adapt() {
            return (List<Event>) data;
        }
    }

    @SuppressWarnings("unchecked")
    private static class SpreadCollection extends MetricCollection {
        SpreadCollection(List<? extends Metric> spread) {
            super(MetricType.SPREAD, spread);
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateSpreads(tags, series, adapt());
        }

        private List<Spread> adapt() {
            return (List<Spread>) data;
        }
    }

    @SuppressWarnings("unchecked")
    private static class GroupCollection extends MetricCollection {
        GroupCollection(List<? extends Metric> groups) {
            super(MetricType.GROUP, groups);
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updateGroup(tags, series, adapt());
        }

        private List<MetricGroup> adapt() {
            return (List<MetricGroup>) data;
        }
    }

    @SuppressWarnings("unchecked")
    private static class CardinalityCollection extends MetricCollection {
        CardinalityCollection(List<? extends Metric> cardinality) {
            super(MetricType.CARDINALITY, cardinality);
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> tags, Set<Series> series
        ) {
            session.updatePayload(tags, series, adapt());
        }

        private List<Payload> adapt() {
            return (List<Payload>) data;
        }
    }
}
