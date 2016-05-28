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
import com.spotify.heroic.aggregation.AggregationOutput;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationSession;
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class FilterAggregation implements AggregationInstance {
    private final AggregationInstance of;
    private final FilterStrategy filterStrategy;

    public FilterAggregation(final FilterStrategy filterStrategy, final AggregationInstance of) {
        this.filterStrategy = checkNotNull(filterStrategy, "filterStrategy");
        this.of = checkNotNull(of, "of");
    }

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
    public AggregationSession session(DateRange range) {
        final AggregationSession child = of.session(range);
        return new Session(filterStrategy, child);
    }

    @Override
    public ReducerSession reducer(DateRange range) {
        return new FilterKReducerSession(filterStrategy, of.reducer(range));
    }

    @Override
    public AggregationCombiner combiner(DateRange range) {
        return all -> {
            final List<FilterableMetrics<ShardedResultGroup>> filterableMetrics = of
                .combiner(range)
                .combine(all)
                .stream()
                .map(s -> new FilterableMetrics<>(s, s::getGroup))
                .collect(Collectors.toList());

            return filterStrategy.filter(filterableMetrics);
        };
    }

    private class Session implements AggregationSession {
        private final AggregationSession childSession;
        private final FilterStrategy filterStrategy;

        public Session(FilterStrategy filterStrategy, AggregationSession childSession) {
            this.filterStrategy = filterStrategy;
            this.childSession = childSession;
        }

        @Override
        public void updatePoints(
            Map<String, String> key, Set<Series> series, List<Point> values
        ) {
            childSession.updatePoints(key, series, values);
        }

        @Override
        public void updateEvents(
            Map<String, String> key, Set<Series> series, List<Event> values
        ) {
            childSession.updateEvents(key, series, values);
        }

        @Override
        public void updateSpreads(
            Map<String, String> key, Set<Series> series, List<Spread> values
        ) {
            childSession.updateSpreads(key, series, values);
        }

        @Override
        public void updateGroup(
            Map<String, String> key, Set<Series> series, List<MetricGroup> values
        ) {
            childSession.updateGroup(key, series, values);
        }

        @Override
        public AggregationResult result() {
            final AggregationResult result = childSession.result();

            final List<FilterableMetrics<AggregationOutput>> filterable = result
                .getResult()
                .stream()
                .map(a -> new FilterableMetrics<>(a, a::getMetrics))
                .collect(Collectors.toList());

            return new AggregationResult(filterStrategy.filter(filterable), result.getStatistics());
        }
    }

    private static class FilterKReducerSession implements ReducerSession {
        private final ReducerSession childReducer;
        private final FilterStrategy filterStrategy;

        public FilterKReducerSession(FilterStrategy filterStrategy, ReducerSession reducer) {
            this.filterStrategy = filterStrategy;
            this.childReducer = reducer;
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
            final List<FilterableMetrics<MetricCollection>> filterableMetrics = childReducer
                .result()
                .getResult()
                .stream()
                .map(m -> new FilterableMetrics<>(m, () -> m))
                .collect(Collectors.toList());

            return new ReducerResult(filterStrategy.filter(filterableMetrics),
                childReducer.result().getStatistics());
        }
    }
}
