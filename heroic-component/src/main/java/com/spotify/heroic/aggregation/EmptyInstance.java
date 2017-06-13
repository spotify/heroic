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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.Data;
import lombok.ToString;

@Data
public class EmptyInstance implements AggregationInstance {
    public static final Map<String, String> EMPTY_GROUP = ImmutableMap.of();
    public static final EmptyInstance INSTANCE = new EmptyInstance();

    @Override
    public long estimate(DateRange range) {
        return -1;
    }

    @Override
    public AggregationSession session(
        final DateRange range, final RetainQuotaWatcher watcher, final BucketStrategy bucketStrategy
    ) {
        return new CollectorSession(watcher);
    }

    @Override
    public AggregationInstance distributed() {
        return this;
    }

    @Override
    public long cadence() {
        return 0;
    }

    /**
     * A trivial session that collects all values provided to it.
     */
    @Data
    @ToString(of = {})
    private static final class CollectorSession implements AggregationSession {
        private final ConcurrentMap<Map<String, String>, SubSession> sessions =
            new ConcurrentHashMap<>();
        private final Object lock = new Object();
        private final RetainQuotaWatcher quotaWatcher;

        @Override
        public void updatePoints(
            Map<String, String> key, Set<Series> s, List<Point> values
        ) {
            quotaWatcher.retainData(values.size());
            session(key).points.update(s, values);
        }

        @Override
        public void updateEvents(
            Map<String, String> key, Set<Series> s, List<Event> values
        ) {
            quotaWatcher.retainData(values.size());
            session(key).events.update(s, values);
        }

        @Override
        public void updateSpreads(
            Map<String, String> key, Set<Series> s, List<Spread> values
        ) {
            quotaWatcher.retainData(values.size());
            session(key).spreads.update(s, values);
        }

        @Override
        public void updateGroup(
            Map<String, String> key, Set<Series> s, List<MetricGroup> values
        ) {
            quotaWatcher.retainData(values.size());
            session(key).groups.update(s, values);
        }

        @Override
        public void updatePayload(
            final Map<String, String> key, final Set<Series> s, final List<Payload> values
        ) {
            quotaWatcher.retainData(values.size());
            session(key).cardinality.update(s, values);
        }

        private SubSession session(Map<String, String> key) {
            final SubSession session = sessions.get(key);

            if (session != null) {
                return session;
            }

            synchronized (lock) {
                final SubSession checkSession = sessions.get(key);

                if (checkSession != null) {
                    return checkSession;
                }

                final SubSession newSession = new SubSession();
                sessions.put(key, newSession);
                return newSession;
            }
        }

        @Override
        public AggregationResult result() {
            final ImmutableList.Builder<AggregationOutput> groups = ImmutableList.builder();

            for (final Map.Entry<Map<String, String>, SubSession> e : sessions.entrySet()) {
                final Map<String, String> group = e.getKey();
                final SubSession sub = e.getValue();

                if (!sub.groups.isEmpty()) {
                    groups.add(collectGroup(group, sub.groups, MetricCollection::groups));
                }

                if (!sub.points.isEmpty()) {
                    groups.add(collectGroup(group, sub.points, MetricCollection::points));
                }

                if (!sub.events.isEmpty()) {
                    groups.add(collectGroup(group, sub.events, MetricCollection::events));
                }

                if (!sub.spreads.isEmpty()) {
                    groups.add(collectGroup(group, sub.spreads, MetricCollection::spreads));
                }
            }

            return new AggregationResult(groups.build(), Statistics.empty());
        }

        private <T extends Metric> AggregationOutput collectGroup(
            final Map<String, String> key, final SessionPair<T> collected,
            final Function<List<T>, MetricCollection> builder
        ) {
            final ImmutableList.Builder<List<T>> iterables = ImmutableList.builder();

            for (final List<T> d : collected.data) {
                iterables.add(d);
            }

            final Set<Series> series = ImmutableSet.copyOf(Iterables.concat(collected.series));

            /* no need to merge, single results are already sorted */
            if (collected.data.size() == 1) {
                return new AggregationOutput(key, series,
                    builder.apply(iterables.build().iterator().next()));
            }

            final ImmutableList<Iterator<T>> iterators =
                ImmutableList.copyOf(iterables.build().stream().map(Iterable::iterator).iterator());
            final Iterator<T> metrics = Iterators.mergeSorted(iterators, Metric.comparator());

            return new AggregationOutput(key, series, builder.apply(ImmutableList.copyOf(metrics)));
        }
    }

    static class SessionPair<T extends Metric> {
        private final ConcurrentLinkedQueue<Set<Series>> series = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<List<T>> data = new ConcurrentLinkedQueue<>();

        public void update(final Set<Series> s, final List<T> values) {
            series.add(s);
            data.add(values);
        }

        public boolean isEmpty() {
            return data.isEmpty();
        }
    }

    static class SubSession {
        private final SessionPair<Point> points = new SessionPair<>();
        private final SessionPair<Event> events = new SessionPair<>();
        private final SessionPair<Spread> spreads = new SessionPair<>();
        private final SessionPair<MetricGroup> groups = new SessionPair<>();
        private final SessionPair<Payload> cardinality = new SessionPair<>();
    }
}
