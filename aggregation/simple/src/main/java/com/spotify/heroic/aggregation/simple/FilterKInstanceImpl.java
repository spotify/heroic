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

package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.aggregation.AggregationTraversal;
import com.spotify.heroic.aggregation.ReducerResult;
import com.spotify.heroic.aggregation.ReducerSession;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Spread;
import lombok.Data;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class FilterKInstanceImpl implements FilterKInstance {
    private final AggregationInstance of;

    public enum FilterType {
        TOP,
        BOTTOM
    }
    private final long k;

    private final FilterType filterType;
    public FilterKInstanceImpl(long k, FilterType filterType, final AggregationInstance of) {
        this.k = k;
        this.filterType = checkNotNull(filterType, "filterType");
        this.of = checkNotNull(of, "of");
    }

    @Override
    public long getK() {
        return k;
    }

    @Override
    public AggregationInstance getOf() {
        return of;
    }

    @Override
    public long estimate(DateRange range) {
        return 0;
    }

    @Override
    public long cadence() {
        return of.cadence();
    }

    @Override
    public AggregationTraversal session(List<AggregationState> states, DateRange range) {
        final AggregationSession child = of.session(states, range).getSession();
        return new AggregationTraversal(states, new Session(k, filterType, child));
    }

    @Override
    public ReducerSession reducer(DateRange range) {
        return new FilterKReducerSession(of.reducer(range), k, filterType);
    }

    @Override
    public AggregationCombiner combiner(DateRange range) {
        return all -> {
            final List<FilterableKMetrics<ShardedResultGroup>> filterableMetrics =
                of.combiner(range)
                    .combine(all)
                    .stream()
                    .map(s -> new FilterableKMetrics<>(s, s::getGroup))
                    .collect(Collectors.toList());

            return FilterK.filter(filterableMetrics, filterType, k);
        };
    }

    private static class Session implements AggregationSession {
        private final long k;
        private final FilterType filterType;
        private final AggregationSession childSession;

        public Session(long k,
                       FilterType filterType,
                       AggregationSession childSession) {
            this.k = k;
            this.filterType = filterType;
            this.childSession = childSession;
        }

        @Override
        public void updatePoints(Map<String, String> group, Set<Series> series,
                                 List<Point> values) {
            childSession.updatePoints(group, series, values);
        }

        @Override
        public void updateEvents(Map<String, String> group, Set<Series> series,
                                 List<Event> values) {
            childSession.updateEvents(group, series, values);
        }

        @Override
        public void updateSpreads(Map<String, String> group, Set<Series> series,
                                  List<Spread> values) {
            childSession.updateSpreads(group, series, values);
        }

        @Override
        public void updateGroup(Map<String, String> group, Set<Series> series,
                                List<MetricGroup> values) {
            childSession.updateGroup(group, series, values);
        }

        @Override
        public AggregationResult result() {
            final List<AggregationData> result = FilterK.filter(getFilterableAggregationData(),
                filterType, k);

            return new AggregationResult(result, childSession.result().getStatistics());
        }

        private List<FilterableKMetrics<AggregationData>> getFilterableAggregationData() {
            return childSession.result().getResult()
                .stream()
                .map(a -> new AggregationData(a.getGroup(), a.getSeries(), a.getMetrics()))
                .map(a -> new FilterableKMetrics<>(a, a::getMetrics))
                .collect(Collectors.toList());
        }
    }

    private static class FilterKReducerSession implements ReducerSession {
        private final ReducerSession childReducer;
        private final long k;
        private final FilterType filterType;

        public FilterKReducerSession(ReducerSession reducer, long k, FilterType filterType) {
            this.childReducer = reducer;
            this.k = k;
            this.filterType = filterType;
        }

        @Override
        public void updatePoints(Map<String, String> group, List<Point> values) {
            childReducer.updatePoints(group, values);
        }

        @Override
        public void updateEvents(Map<String, String> group, List<Event> values) {
            childReducer.updateEvents(group, values);
        }

        @Override
        public void updateSpreads(Map<String, String> group, List<Spread> values) {
            childReducer.updateSpreads(group, values);
        }

        @Override
        public void updateGroup(Map<String, String> group, List<MetricGroup> values) {
            childReducer.updateGroup(group, values);
        }

        @Override
        public ReducerResult result() {
            final List<FilterableKMetrics<MetricCollection>> filterableKMetrics =
                childReducer.result()
                    .getResult()
                    .stream()
                    .map(m -> new FilterableKMetrics<>(m, () -> m))
                    .collect(Collectors.toList());

            return new ReducerResult(FilterK.filter(filterableKMetrics, filterType, k),
                childReducer.result().getStatistics());
        }
    }

    @Data
    private static class FilterableKMetrics<T> {
        private final T data;
        private final Supplier<MetricCollection> metricSupplier;
    }

    private static class FilterK<T> {
        private final List<FilterableKMetrics<T>> metrics;
        private final FilterType filterType;
        private final long k;

        public static <T> List<T> filter(List<FilterableKMetrics<T>> metrics,
                                         FilterType filterType, long k) {
            return new FilterK<>(metrics, filterType, k).apply();
        }

        private FilterK(List<FilterableKMetrics<T>> metrics, FilterType filterType, long k) {
            this.metrics = metrics;
            this.filterType = filterType;
            this.k = k;
        }

        private List<T> apply() {
            return metrics.stream()
                .map(Area::new)
                .sorted(buildCompare(filterType))
                .limit(k)
                .map(Area::getFilterableKMetrics)
                .map(FilterableKMetrics::getData)
                .collect(Collectors.toList());
        }

        private Comparator<Area> buildCompare(FilterType filterType) {
            return (Area a, Area b) -> {
                final Comparator<Double> comparator = Double::compare;

                if (filterType == FilterType.TOP) {
                    return comparator.reversed().compare(a.getValue(), b.getValue());
                } else {
                    return comparator.compare(a.getValue(), b.getValue());
                }
            };
        }

        @Data
        private class Area {
            private final FilterableKMetrics<T> filterableKMetrics;
            private final double value;

            public Area(FilterableKMetrics<T> filterableKMetrics) {
                this.filterableKMetrics = filterableKMetrics;
                this.value = computeArea(filterableKMetrics.getMetricSupplier().get());
            }

            private Double computeArea(MetricCollection metrics) {
                return metrics.getDataAs(Point.class)
                    .stream()
                    .map(Point::getValue)
                    .reduce(0D, (a, b) -> a + b);
            }
        }
    }

}
