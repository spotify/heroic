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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Spread;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
@EqualsAndHashCode(of = {"of", "each"})
public abstract class GroupingAggregation implements AggregationInstance {
    private final Optional<List<String>> of;
    private final AggregationInstance each;

    public GroupingAggregation(final Optional<List<String>> of, final AggregationInstance each) {
        this.of = checkNotNull(of, "of");
        this.each = checkNotNull(each, "each");
    }

    /**
     * Generate a key for the specific group.
     *
     * @param input The input tags for the group.
     * @return The keys for a specific group.
     */
    protected abstract Map<String, String> key(final Map<String, String> input);

    /**
     * Create a new instance of this aggregation.
     */
    protected abstract AggregationInstance newInstance(
        final Optional<List<String>> of, final AggregationInstance each
    );

    @Override
    public AggregationSession session(DateRange range) {
        return new GroupSession(range);
    }

    public Set<String> requiredTags() {
        return of.map(ImmutableSet::copyOf).orElseGet(ImmutableSet::of);
    }

    @Override
    public long estimate(DateRange range) {
        return each.estimate(range);
    }

    @Override
    public long cadence() {
        return each.cadence();
    }

    @Override
    public AggregationInstance distributed() {
        return newInstance(of, each.distributed());
    }

    @Override
    public AggregationSession reducer(final DateRange range) {
        return each.reducer(range);
    }

    /**
     * Grouping aggregations need to recombine the results for each result group.
     * <p>
     * Result groups are identified by their tags, and several shards might return groups having the
     * same id. Because of this, each group must instantiate an {@link
     * AggregationInstance#reducer(DateRange)}, belonging to the child aggregation to recombine
     * results that are from different groups.
     */
    @Override
    public AggregationCombiner combiner(final DateRange range) {
        return (all) -> {
            final Map<Map<String, String>, Reduction> sessions = new HashMap<>();

            /* iterate through all groups and setup, and feed a reducer session for every group */
            for (List<ShardedResultGroup> groups : all) {
                for (final ShardedResultGroup g : groups) {
                    final Map<String, String> key = g.getKey();

                    Reduction red = sessions.get(key);

                    if (red == null) {
                        red = new Reduction(each.reducer(range));
                        sessions.put(key, red);
                    }

                    g.getGroup().updateAggregation(red.session, key, ImmutableSet.of());
                    red.series.addSeriesValues(g.getSeries());
                }
            }

            /* build results from every reducer group into a final result */
            final ImmutableList.Builder<ShardedResultGroup> groups = ImmutableList.builder();

            for (final Map.Entry<Map<String, String>, Reduction> e : sessions.entrySet()) {
                final Map<String, String> key = e.getKey();
                final Reduction red = e.getValue();

                final SeriesValues series = red.series.build();
                final AggregationResult result = red.session.result();

                for (final AggregationOutput out : result.getResult()) {
                    groups.add(
                        new ShardedResultGroup(ImmutableMap.of(), key, series, out.getMetrics(),
                            each.cadence()));
                }
            }

            return groups.build();
        };
    }

    @Override
    public String toString() {
        return String.format("%s(of=%s, each=%s)", getClass().getSimpleName(), of, each);
    }

    @Data
    private final class Reduction {
        private final AggregationSession session;
        private final SeriesValues.Builder series = SeriesValues.builder();
    }

    @ToString
    @RequiredArgsConstructor
    private final class GroupSession implements AggregationSession {
        private final ConcurrentMap<Map<String, String>, AggregationSession> sessions =
            new ConcurrentHashMap<>();
        private final Object lock = new Object();

        private final DateRange range;

        @Override
        public void updatePoints(
            Map<String, String> group, Set<Series> series, List<Point> values
        ) {
            final Map<String, String> key = key(group);
            session(key).updatePoints(key, series, values);
        }

        @Override
        public void updateEvents(
            Map<String, String> group, Set<Series> series, List<Event> values
        ) {
            final Map<String, String> key = key(group);
            session(key).updateEvents(key, series, values);
        }

        @Override
        public void updateSpreads(
            Map<String, String> group, Set<Series> series, List<Spread> values
        ) {
            final Map<String, String> key = key(group);
            session(key).updateSpreads(key, series, values);
        }

        @Override
        public void updateGroup(
            Map<String, String> group, Set<Series> series, List<MetricGroup> values
        ) {
            final Map<String, String> key = key(group);
            session(key).updateGroup(key, series, values);
        }

        private AggregationSession session(final Map<String, String> key) {
            final AggregationSession session = sessions.get(key);

            if (session != null) {
                return session;
            }

            synchronized (lock) {
                final AggregationSession checkSession = sessions.get(key);

                if (checkSession != null) {
                    return checkSession;
                }

                final AggregationSession newSession = each.session(range);
                sessions.put(key, newSession);
                return newSession;
            }
        }

        @Override
        public AggregationResult result() {
            final ImmutableList.Builder<AggregationOutput> result = ImmutableList.builder();

            Statistics statistics = Statistics.empty();

            for (final Map.Entry<Map<String, String>, AggregationSession> e : sessions.entrySet()) {
                final Map<String, String> key = e.getKey();
                final AggregationResult a = e.getValue().result();

                for (final AggregationOutput d : a.getResult()) {
                    result.add(d.withKey(key));
                }

                statistics = a.getStatistics().merge(statistics);
            }

            return new AggregationResult(result.build(), statistics);
        }
    }
}
