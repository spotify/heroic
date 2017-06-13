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

        private final BucketStrategy.Mapping mapping;
        private final List<B> buckets;

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

                final BucketStrategy.StartEnd startEnd = mapping.map(m.getTimestamp());

                for (int i = startEnd.getStart(); i < startEnd.getEnd(); i++) {
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
    }

    @Override
    public long estimate(DateRange original) {
        if (size == 0) {
            return 0;
        }

        return original.rounded(size).diff() / size;
    }

    @Override
    public Session session(
        DateRange range, RetainQuotaWatcher quotaWatcher, BucketStrategy bucketStrategy
    ) {
        final BucketStrategy.Mapping mapping = bucketStrategy.setup(range, size, extent);
        final List<B> buckets = buildBuckets(mapping);
        quotaWatcher.retainData(buckets.size());
        return new Session(mapping, buckets);
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

    private List<B> buildBuckets(final BucketStrategy.Mapping mapping) {
        final List<B> buckets = new ArrayList<>(mapping.buckets());

        for (int i = 0; i < mapping.buckets(); i++) {
            buckets.add(buildBucket(mapping.start() + size * i));
        }

        return buckets;
    }

    protected abstract B buildBucket(long timestamp);

    protected abstract Metric build(B bucket);

    private interface BucketConsumer<B extends Bucket, M extends Metric> {
        void apply(B bucket, M metric);
    }
}
