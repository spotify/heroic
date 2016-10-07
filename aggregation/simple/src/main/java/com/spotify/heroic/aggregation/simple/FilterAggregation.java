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

import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationOutput;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.EmptyInstance;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class FilterAggregation implements AggregationInstance {
    private final FilterStrategy filterStrategy;

    public FilterAggregation(final FilterStrategy filterStrategy) {
        this.filterStrategy = checkNotNull(filterStrategy, "filterStrategy");
    }

    @Override
    public long estimate(DateRange range) {
        return -1;
    }

    @Override
    public long cadence() {
        return -1;
    }

    @Override
    public AggregationInstance distributed() {
        return EmptyInstance.INSTANCE;
    }

    @Override
    public AggregationInstance reducer() {
        return EmptyInstance.INSTANCE;
    }

    /**
     * Filtering aggregations are by definition <em>not</em> distributable since they are incapable
     * of making a complete local decision.
     */
    @Override
    public boolean distributable() {
        return false;
    }

    @Override
    public AggregationSession session(DateRange range) {
        return new Session(filterStrategy, EmptyInstance.INSTANCE.session(range));
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
        public void updatePayload(
            Map<String, String> key, Set<Series> series, List<Payload> values
        ) {
            childSession.updatePayload(key, series, values);
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
}
