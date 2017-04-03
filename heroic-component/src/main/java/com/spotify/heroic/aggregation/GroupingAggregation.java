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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

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
    public AggregationSession session(
        DateRange range, RetainQuotaWatcher quotaWatcher, BucketStrategy bucketStrategy
    ) {
        return new GroupSession(range, quotaWatcher, bucketStrategy);
    }

    public Set<String> requiredTags() {
        return of.map(ImmutableSet::copyOf).orElseGet(ImmutableSet::of);
    }

    @Override
    public long estimate(DateRange range) {
        return -1;
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
    public AggregationInstance reducer() {
        return newInstance(of, each.reducer());
    }

    @Override
    public String toString() {
        return String.format("%s(of=%s, each=%s)", getClass().getSimpleName(), of, each);
    }

    @ToString
    @RequiredArgsConstructor
    private final class GroupSession implements AggregationSession {
        private final ConcurrentMap<Map<String, String>, AggregationSession> sessions =
            new ConcurrentHashMap<>();
        private final Object lock = new Object();

        private final DateRange range;
        private final RetainQuotaWatcher quotaWatcher;
        private final BucketStrategy bucketStrategy;

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

        @Override
        public void updatePayload(
            Map<String, String> group, Set<Series> series, List<Payload> values
        ) {
            final Map<String, String> key = key(group);
            session(key).updatePayload(key, series, values);
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

                final AggregationSession newSession =
                    each.session(range, quotaWatcher, bucketStrategy);
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
