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

package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class PartitionInstance implements AggregationInstance {
    private final List<AggregationInstance> children;

    @JsonCreator
    public PartitionInstance(@JsonProperty("children") final List<AggregationInstance> children) {
        this.children = checkNotNull(children, "children");
    }

    @Override
    public AggregationTraversal session(List<AggregationState> states, DateRange range) {
        final ImmutableList.Builder<AggregationState> childStates = ImmutableList.builder();
        final ImmutableList.Builder<AggregationSession> sessions = ImmutableList.builder();

        for (final AggregationInstance a : children) {
            final AggregationTraversal traversal = a.session(states, range);
            childStates.addAll(traversal.getStates());
            sessions.add(traversal.getSession());
        }

        final AggregationSession session = new PartitionSession(sessions.build());
        return new AggregationTraversal(childStates.build(), session);
    }

    @Override
    public long estimate(DateRange range) {
        long estimate = 0;

        for (final AggregationInstance a : children) {
            estimate += a.estimate(range);
        }

        return estimate;
    }

    @Override
    public long cadence() {
        long cadence = 0;

        for (final AggregationInstance a : children) {
            cadence = Math.max(cadence, a.cadence());
        }

        return cadence;
    }

    @Override
    public ReducerSession reducer(DateRange range) {
        final ImmutableList.Builder<ReducerSession> sessions = ImmutableList.builder();

        for (final AggregationInstance a : children) {
            sessions.add(a.reducer(range));
        }

        return new PartitionReducerSession(sessions.build());
    }

    @ToString
    @RequiredArgsConstructor
    private final class PartitionSession implements AggregationSession {
        private final List<AggregationSession> sessions;

        @Override
        public void updatePoints(
            Map<String, String> group, List<Point> values
        ) {
            for (final AggregationSession s : sessions) {
                s.updatePoints(group, values);
            }
        }

        @Override
        public void updateEvents(
            Map<String, String> group, List<Event> values
        ) {
            for (final AggregationSession s : sessions) {
                s.updateEvents(group, values);
            }
        }

        @Override
        public void updateSpreads(
            Map<String, String> group, List<Spread> values
        ) {
            for (final AggregationSession s : sessions) {
                s.updateSpreads(group, values);
            }
        }

        @Override
        public void updateGroup(
            Map<String, String> group, List<MetricGroup> values
        ) {
            for (final AggregationSession s : sessions) {
                s.updateGroup(group, values);
            }
        }

        @Override
        public AggregationResult result() {
            final ImmutableList.Builder<AggregationData> data = ImmutableList.builder();
            Statistics statistics = Statistics.empty();

            for (final AggregationSession s : sessions) {
                final AggregationResult r = s.result();
                data.addAll(r.getResult());
                statistics = statistics.merge(r.getStatistics());
            }

            return new AggregationResult(data.build(), statistics);
        }
    }

    @ToString
    @RequiredArgsConstructor
    private final class PartitionReducerSession implements ReducerSession {
        private final List<ReducerSession> sessions;

        @Override
        public void updatePoints(Map<String, String> group, List<Point> values) {
            for (final ReducerSession s : sessions) {
                s.updatePoints(group, values);
            }
        }

        @Override
        public void updateEvents(Map<String, String> group, List<Event> values) {
            for (final ReducerSession s : sessions) {
                s.updateEvents(group, values);
            }
        }

        @Override
        public void updateSpreads(Map<String, String> group, List<Spread> values) {
            for (final ReducerSession s : sessions) {
                s.updateSpreads(group, values);
            }
        }

        @Override
        public void updateGroup(Map<String, String> group, List<MetricGroup> values) {
            for (final ReducerSession s : sessions) {
                s.updateGroup(group, values);
            }
        }

        @Override
        public ReducerResult result() {
            final ImmutableList.Builder<MetricCollection> data = ImmutableList.builder();
            Statistics statistics = Statistics.empty();

            for (final ReducerSession s : sessions) {
                final ReducerResult r = s.result();
                data.addAll(r.getResult());
                statistics = statistics.merge(r.getStatistics());
            }

            return new ReducerResult(data.build(), statistics);
        }
    }
}
