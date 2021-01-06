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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.Series;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * A collection of metrics.
 * <p>
 * Metrics are constrained to the implemented types below, so far these are {@link Point},
 * {@link Spread} , and {@link MetricGroup}.
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
 * @see MetricGroup
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(MetricCollection.PointCollection.class),
    @JsonSubTypes.Type(MetricCollection.SpreadCollection.class),
    @JsonSubTypes.Type(MetricCollection.GroupCollection.class),
    @JsonSubTypes.Type(MetricCollection.CardinalityCollection.class),
    @JsonSubTypes.Type(MetricCollection.DistributionPointCollection.class),
    @JsonSubTypes.Type(MetricCollection.TDigestPointCollection.class),
})
public interface MetricCollection {
    /**
     * Get the underlying data.
     */
    List<? extends Metric> data();

    /**
     * Get the type of the encapsulated data.
     */
    @JsonIgnore
    MetricType getType();

    /**
     * Update the given aggregation with the content of this collection.
     */
    void updateAggregation(
        AggregationSession session, Map<String, String> key, Set<Series> series
    );

    /**
     * Helper method to fetch a collection of the given type, if applicable.
     * <p>
     * This API is not safe, checks must be performed to verify that the encapsulated data type is
     * the same as expected.
     *
     * @param expected The expected type to read.
     * @return a list of the expected type.
     */
    @SuppressWarnings("unchecked")
    @JsonIgnore
    default <T> List<T> getDataAs(Class<T> expected) {
        if (expected == getType().type()) {
            return (List<T>) data();
        }

        throw new RuntimeException("collection not of type: " + expected);
    }

    /**
     * Get the size of the collection.
     *
     * @return size of the collection
     */
    default int size() {
        return data().size();
    }

    /**
     * Check if the collection is empty.
     *
     * @return {@code true} if the collection is empty
     */
    @JsonIgnore
    default boolean isEmpty() {
        return data().isEmpty();
    }

    @JsonIgnore
    default Optional<Long> getAverageDistanceBetweenMetrics() {
        final List<? extends Metric> data = data();

        if (data.size() <= 1) {
            return Optional.empty();
        }

        final long timeDiff = data.get(data.size() - 1).getTimestamp() - data.get(0).getTimestamp();
        final long spans = data.size() - 1;

        return Optional.of(timeDiff / spans);
    }

    static MetricCollection groups(List<MetricGroup> metrics) {
        return GroupCollection.create(metrics);
    }

    /**
     * Build a new points collection.
     *
     * @param metrics metrics to include in the collection
     * @return a new points collection
     */
    static MetricCollection points(List<Point> metrics) {
        return PointCollection.create(metrics);
    }

    /**
     * Create a new distribution point collection
     *
     * @param metric distribution points
     * @return a new collection of distribution point
     */
    static MetricCollection distributionPoints(List<DistributionPoint> metric) {
        return DistributionPointCollection.create(metric);
    }

    /**
     * Build a new spreads collection.
     *
     * @param metrics spreads to include in the collection
     * @return a new spreads collection
     */
    static MetricCollection spreads(List<Spread> metrics) {
        return SpreadCollection.create(metrics);
    }

    /**
     * Build a new cardinality collection.
     *
     * @param metrics cardinality samples to include in the collection
     * @return a new cardinality collection
     */
    static MetricCollection cardinality(List<Payload> metrics) {
        return CardinalityCollection.create(metrics);
    }

    /**
     * Build a new metric collection of the given type.
     * <p>
     * This API is not safe, checks must be performed to verify that the encapsulated data type is
     * correct.
     */
    @SuppressWarnings("unchecked")
    static MetricCollection build(
        final MetricType key, final List<? extends Metric> metrics
    ) {
        switch (key) {
            case CARDINALITY:
                return CardinalityCollection.create((List<Payload>) metrics);
            case GROUP:
                return GroupCollection.create((List<MetricGroup>) metrics);
            case SPREAD:
                return SpreadCollection.create((List<Spread>) metrics);
            case POINT:
                return PointCollection.create((List<Point>) metrics);
            case DISTRIBUTION_POINTS:
                return DistributionPointCollection.create((List<DistributionPoint>) metrics);
            case TDIGEST_POINT:
                return TDigestPointCollection.create((List<TdigestPoint>) metrics);
            default:
                throw new RuntimeException("unsupported metric collection");
        }
    }

    /**
     * Merge the given collections and return a new metric collection with the sorted values.
     * <p>
     * This expects the source collections being merged to be sorted.
     * <p>
     * This API is not safe, checks must be performed to verify that the encapsulated data type is
     * the same as expected.
     */
    static MetricCollection mergeSorted(
        final MetricType type, final List<List<? extends Metric>> values
    ) {
        final List<Metric> data = ImmutableList.copyOf(Iterators.mergeSorted(
            ImmutableList.copyOf(values.stream().map(Iterable::iterator).iterator()),
            Metric.comparator));
        return build(type, data);
    }

    @AutoValue
    @JsonTypeName("points")
    abstract class PointCollection implements MetricCollection {
        @JsonCreator
        public static PointCollection create(@JsonProperty("data") final List<Point> data) {
            return new AutoValue_MetricCollection_PointCollection(data);
        }

        @JsonProperty
        public abstract  List<Point> data();

        @Override
        public MetricType getType() {
            return MetricType.POINT;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> key, Set<Series> series
        ) {
            session.updatePoints(key, series, data());
        }
    }

    @AutoValue
    @JsonTypeName("distributionPoints")
    abstract class DistributionPointCollection implements MetricCollection {
        @JsonCreator
        public static DistributionPointCollection create(
            @JsonProperty("data") final List<DistributionPoint> data) {
            return new AutoValue_MetricCollection_DistributionPointCollection(data);
        }

        @JsonProperty
        public abstract  List<DistributionPoint> data();

        @Override
        public MetricType getType() {
            return MetricType.DISTRIBUTION_POINTS;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> key, Set<Series> series
        ) {
            session.updateDistributionPoints(key, series, data());
        }
    }


    @AutoValue
    @JsonTypeName("tdigestPoints")
    abstract class TDigestPointCollection implements MetricCollection {
        @JsonCreator
        public static TDigestPointCollection create(
            @JsonProperty("data") final List<TdigestPoint> data) {
            return new AutoValue_MetricCollection_TDigestPointCollection(data);
        }

        @JsonProperty
        public abstract  List<TdigestPoint> data();

        @Override
        public MetricType getType() {
            return MetricType.TDIGEST_POINT;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> key, Set<Series> series
        ) {
            session.updateTDigestPoints(key, series, data());
        }
    }


    @AutoValue
    @JsonTypeName("spreads")
    abstract class SpreadCollection implements MetricCollection {
        @JsonCreator
        public static SpreadCollection create(@JsonProperty("data") final List<Spread> data) {
            return new AutoValue_MetricCollection_SpreadCollection(data);
        }

        @JsonProperty
        public abstract List<Spread> data();

        @Override
        public MetricType getType() {
            return MetricType.SPREAD;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> key, Set<Series> series
        ) {
            session.updateSpreads(key, series, data());
        }
    }

    @AutoValue
    @JsonTypeName("groups")
    abstract class GroupCollection implements MetricCollection {
        @JsonCreator
        public static GroupCollection create(@JsonProperty("data") final List<MetricGroup> data) {
            return new AutoValue_MetricCollection_GroupCollection(data);
        }

        @JsonProperty
        public abstract List<MetricGroup> data();

        @Override
        public MetricType getType() {
            return MetricType.GROUP;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> key, Set<Series> series
        ) {
            session.updateGroup(key, series, data());
        }
    }

    @AutoValue
    @JsonTypeName("cardinality")
    abstract class CardinalityCollection implements MetricCollection {
        @JsonCreator
        public static CardinalityCollection create(@JsonProperty("data") final List<Payload> data) {
            return new AutoValue_MetricCollection_CardinalityCollection(data);
        }

        @JsonProperty
        public abstract List<Payload> data();

        @Override
        public MetricType getType() {
            return MetricType.CARDINALITY;
        }

        @Override
        public void updateAggregation(
            AggregationSession session, Map<String, String> key, Set<Series> series
        ) {
            session.updatePayload(key, series, data());
        }
    }
}
