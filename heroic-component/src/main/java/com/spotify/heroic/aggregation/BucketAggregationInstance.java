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
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A base aggregation that collects data in 'buckets', one for each sampled data point.
 * <p>
 * A bucket aggregation is used to down-sample a lot of data into distinct buckets over time, making
 * them useful for presentation purposes. Buckets have to be thread safe.
 *
 * @param <B> The bucket type.
 * @author udoprog
 * @see Bucket
 */
@Data
@EqualsAndHashCode(of = {"size", "extent"})
public abstract class BucketAggregationInstance<B extends Bucket> implements AggregationInstance {
    public static final Map<String, String> EMPTY_KEY = ImmutableMap.of();
    public static final long MAX_BUCKET_COUNT = 100000L;

    private static final Map<String, String> EMPTY = ImmutableMap.of();

    public static final Set<MetricType> ALL_TYPES =
        ImmutableSet.of(MetricType.POINT, MetricType.EVENT, MetricType.SPREAD, MetricType.GROUP,
            MetricType.CARDINALITY);

    protected final long size;
    protected final long extent;

    @Getter(AccessLevel.NONE)
    private final Set<MetricType> input;

    @Getter(AccessLevel.NONE)
    protected final MetricType out;

    @Data
    final class Session implements AggregationSession {
        private final ConcurrentLinkedQueue<Set<Series>> series = new ConcurrentLinkedQueue<>();
        private final LongAdder sampleSize = new LongAdder();

        private final List<B> buckets;
        private final long offset;

        @Override
        public void updatePoints(
            Map<String, String> key, Set<Series> s, List<Point> values
        ) {
            series.add(s);
            feed(MetricType.POINT, values, (bucket, m) -> bucket.updatePoint(key, m));
        }

        @Override
        public void updateEvents(
            Map<String, String> key, Set<Series> s, List<Event> values
        ) {
            series.add(s);
            feed(MetricType.EVENT, values, (bucket, m) -> bucket.updateEvent(key, m));
        }

        @Override
        public void updateSpreads(
            Map<String, String> key, Set<Series> s, List<Spread> values
        ) {
            series.add(s);
            feed(MetricType.SPREAD, values, (bucket, m) -> bucket.updateSpread(key, m));
        }

        @Override
        public void updateGroup(
            Map<String, String> key, Set<Series> s, List<MetricGroup> values
        ) {
            series.add(s);
            feed(MetricType.GROUP, values, (bucket, m) -> bucket.updateGroup(key, m));
        }

        @Override
        public void updatePayload(
            Map<String, String> key, Set<Series> s, List<Payload> values
        ) {
            series.add(s);
            feed(MetricType.CARDINALITY, values, (bucket, m) -> bucket.updatePayload(key, m));
        }

        private <T extends Metric> void feed(
            final MetricType type, List<T> values, final BucketConsumer<B, T> consumer
        ) {
            if (!input.contains(type)) {
                return;
            }

            int sampleSize = 0;

            for (final T m : values) {
                if (!m.valid()) {
                    continue;
                }

                final StartEnd startEnd = mapTimestamp(m.getTimestamp());

                for (int i = startEnd.start; i < startEnd.end; i++) {
                    consumer.apply(buckets.get(i), m);
                }

                sampleSize += 1;
            }

            this.sampleSize.add(sampleSize);
        }

        @Override
        public AggregationResult result() {
            final List<Metric> result = new ArrayList<>(buckets.size());

            for (final B bucket : buckets) {
                final Metric d = build(bucket);

                if (!d.valid()) {
                    continue;
                }

                result.add(d);
            }

            final Set<Series> series = ImmutableSet.copyOf(Iterables.concat(this.series));
            final MetricCollection metrics = MetricCollection.build(out, result);

            final Statistics statistics =
                new Statistics(ImmutableMap.of(AggregationInstance.SAMPLE_SIZE, sampleSize.sum()));

            final AggregationOutput d = new AggregationOutput(EMPTY_KEY, series, metrics);
            return new AggregationResult(ImmutableList.of(d), statistics);
        }

        /**
         * Calculate the start and end index of the buckets that should be seeded for the given
         * timestamp.
         *
         * This guarantees that each timestamp ends up in the range (start - extent, end] for any
         * given bucket.
         *
         * @param timestamp timestamp to map
         * @return a start end and index
         */
        protected StartEnd mapTimestamp(final long timestamp) {
            /* adjust the timestamp to the number of buckets */
            final long adjusted = timestamp - offset;

            final int start = Math.max((int) ((adjusted - 1) / size), 0);
            final int end = Math.min((int) ((adjusted + extent - 1) / size), buckets.size());

            return new StartEnd(start, end);
        }
    }

    @Override
    public long estimate(DateRange original) {
        if (size == 0) {
            return 0;
        }

        return original.rounded(size).diff() / size;
    }

    @Override
    public Session session(DateRange range, RetainQuotaWatcher quotaWatcher) {
        final List<B> buckets = buildBuckets(range, size);
        quotaWatcher.retainData(buckets.size());
        return new Session(buckets, range.start());
    }

    @Override
    public AggregationInstance distributed() {
        return this;
    }

    @Override
    public long cadence() {
        return size;
    }

    @Override
    public String toString() {
        return String.format("%s(size=%d, extent=%d)", getClass().getSimpleName(), size, extent);
    }

    private List<B> buildBuckets(final DateRange range, long size) {
        final long start = range.start() + size;
        final long count = (range.diff() + size) / size - 1;

        if (count < 1 || count > MAX_BUCKET_COUNT) {
            throw new IllegalArgumentException(String.format("range %s, size %d", range, size));
        }

        final List<B> buckets = new ArrayList<>((int) count);

        for (int i = 0; i < count; i++) {
            buckets.add(buildBucket(start + size * i));
        }

        return buckets;
    }

    protected abstract B buildBucket(long timestamp);

    protected abstract Metric build(B bucket);

    private interface BucketConsumer<B extends Bucket, M extends Metric> {
        void apply(B bucket, M metric);
    }

    /**
     * A start, and an end bucket (exclusive) selected.
     */
    @Data
    public static class StartEnd {
        private final int start;
        private final int end;
    }
}
