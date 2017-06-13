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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * A special aggregation method that is a chain of other aggregation methods.
 *
 * @author udoprog
 */
@Data
public class ChainInstance implements AggregationInstance {
    private final List<AggregationInstance> chain;

    /**
     * The last aggregation in the chain determines the estimated number of samples.
     */
    @Override
    public long estimate(DateRange range) {
        return chain.get(chain.size() - 1).estimate(range);
    }

    /**
     * The last aggregation in the chain determines the cadence.
     */
    @Override
    public long cadence() {
        return chain
            .stream()
            .map(AggregationInstance::cadence)
            .filter(c -> c >= 0)
            .reduce((a, b) -> b)
            .orElse(-1L);
    }

    @Override
    public AggregationInstance distributed() {
        final Iterator<AggregationInstance> it = chain.iterator();

        final ImmutableList.Builder<AggregationInstance> chain = ImmutableList.builder();
        AggregationInstance last = it.next();

        if (!last.distributable()) {
            return EmptyInstance.INSTANCE;
        }

        while (it.hasNext()) {
            final AggregationInstance next = it.next();

            if (!next.distributable()) {
                chain.add(last.distributed());
                return fromList(chain.build());
            }

            chain.add(last);
            last = next;
        }

        chain.add(last.distributed());
        return fromList(chain.build());
    }

    @Override
    public AggregationInstance reducer() {
        final Iterator<AggregationInstance> it = chain.iterator();
        AggregationInstance last = it.next();

        final ImmutableList.Builder<AggregationInstance> chain = ImmutableList.builder();

        if (!last.distributable()) {
            chain.add(EmptyInstance.INSTANCE);
            chain.add(last);

            while (it.hasNext()) {
                chain.add(it.next());
            }

            return fromList(chain.build());
        }

        while (it.hasNext()) {
            final AggregationInstance next = it.next();

            if (!next.distributable()) {
                chain.add(last.reducer());
                chain.add(next);

                while (it.hasNext()) {
                    chain.add(it.next());
                }

                return fromList(chain.build());
            }

            last = next;
        }

        return last.reducer();
    }

    @Override
    public AggregationSession session(
        final DateRange range, final RetainQuotaWatcher watcher, final BucketStrategy bucketStrategy
    ) {
        final Iterator<AggregationInstance> it = chain.iterator();

        final AggregationInstance first = it.next();
        final AggregationSession head = first.session(range, watcher, bucketStrategy);

        final List<AggregationSession> tail = new ArrayList<>();

        while (it.hasNext()) {
            final AggregationSession s = it.next().session(range, watcher, bucketStrategy);
            tail.add(s);
        }

        return new Session(head, tail);
    }

    @Override
    public Set<String> requiredTags() {
        return chain.iterator().next().requiredTags();
    }

    private static final Joiner CHAIN_JOINER = Joiner.on(" -> ");

    @Override
    public String toString() {
        return "[" + CHAIN_JOINER.join(chain.stream().map(Object::toString).iterator()) + "]";
    }

    public static AggregationInstance of(final AggregationInstance... chain) {
        return fromList(ImmutableList.copyOf(chain));
    }

    public static AggregationInstance fromList(final List<AggregationInstance> chain) {
        final List<AggregationInstance> c = flattenChain(chain);

        if (c.size() == 1) {
            return c.iterator().next();
        }

        return new ChainInstance(c);
    }

    private static List<AggregationInstance> flattenChain(
        final List<AggregationInstance> chain
    ) {
        final ImmutableList.Builder<AggregationInstance> child = ImmutableList.builder();

        for (final AggregationInstance i : chain) {
            if (i instanceof ChainInstance) {
                child.addAll(flattenChain(ChainInstance.class.cast(i).getChain()));
            } else {
                child.add(i);
            }
        }

        return child.build();
    }

    @RequiredArgsConstructor
    private static final class Session implements AggregationSession {
        private final AggregationSession first;
        private final Iterable<AggregationSession> rest;

        @Override
        public void updatePoints(
            Map<String, String> key, Set<Series> series, List<Point> values
        ) {
            first.updatePoints(key, series, values);
        }

        @Override
        public void updateEvents(
            Map<String, String> key, Set<Series> series, List<Event> values
        ) {
            first.updateEvents(key, series, values);
        }

        @Override
        public void updateSpreads(
            Map<String, String> key, Set<Series> series, List<Spread> values
        ) {
            first.updateSpreads(key, series, values);
        }

        @Override
        public void updateGroup(
            Map<String, String> key, Set<Series> series, List<MetricGroup> values
        ) {
            first.updateGroup(key, series, values);
        }

        @Override
        public void updatePayload(
            Map<String, String> key, Set<Series> series, List<Payload> values
        ) {
            first.updatePayload(key, series, values);
        }

        @Override
        public AggregationResult result() {
            final AggregationResult firstResult = first.result();
            List<AggregationOutput> current = firstResult.getResult();
            Statistics statistics = firstResult.getStatistics();

            for (final AggregationSession session : rest) {
                for (final AggregationOutput u : current) {
                    u.getMetrics().updateAggregation(session, u.getKey(), u.getSeries());
                }

                final AggregationResult next = session.result();
                current = next.getResult();
                statistics = statistics.merge(next.getStatistics());
            }

            return new AggregationResult(current, statistics);
        }

        @Override
        public String toString() {
            if (rest.iterator().hasNext()) {
                return "[" + first + ", " + Joiner.on(", ").join(rest) + "]";
            }

            return "[" + first + "]";
        }
    }
}
