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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import lombok.Data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Sampling;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;

/**
 * A base aggregation that collects data in 'buckets', one for each sampled data point.
 *
 * A bucket aggregation is used to down-sample a lot of data into distinct buckets over time, making them useful for
 * presentation purposes. Buckets have to be thread safe.
 *
 * @param <IN> The expected input data type.
 * @param <OUT> The expected output data type.
 * @param <T> The bucket type.
 *
 * @see Bucket
 *
 * @author udoprog
 */
@Data
public abstract class BucketAggregation<IN extends Metric, B extends Bucket<IN>> implements
        Aggregation {
    public static final Map<String, String> EMPTY_GROUP = ImmutableMap.of();
    public static final long MAX_BUCKET_COUNT = 100000l;

    private final static Map<String, String> EMPTY = ImmutableMap.of();

    @Data
    private final class Session implements AggregationSession {
        private final LongAdder sampleSize = new LongAdder();

        private final Set<Series> series;
        private final List<B> buckets;
        private final long offset;
        private final long size;
        private final long extent;

        @SuppressWarnings("unchecked")
        @Override
        public void update(AggregationData update) {
            // ignore incompatible updates
            final MetricType type = update.getType();

            if (in.isAssignableFrom(type.type()))
                return;

            final List<IN> input = (List<IN>) update.getValues();

            for (final IN d : input) {
                if (!d.valid())
                    continue;

                final long first = d.getTimestamp();
                final long last = first + extent;

                for (long start = first; start < last; start += size) {
                    int i = (int) ((start - offset - 1) / size);

                    if (i < 0 || i >= buckets.size())
                        continue;

                    final B bucket = buckets.get(i);

                    final long c = bucket.timestamp() - first;

                    // check that the current bucket is _within_ the extent.
                    if (!(c >= 0 && c <= extent))
                        continue;

                    bucket.update(update.getGroup(), type, d);
                }
            }

            sampleSize.add(input.size());
        }

        @Override
        public AggregationResult result() {
            final List<Metric> result = new ArrayList<>(buckets.size());

            for (final B bucket : buckets) {
                final Metric d = build(bucket);

                if (!d.valid())
                    continue;

                result.add(d);
            }

            final Statistics.Aggregator statistics = new Statistics.Aggregator(sampleSize.sum(), 0l, 0l);
            final List<AggregationData> updates = ImmutableList.of(new AggregationData(EMPTY_GROUP, series, result, out));
            return new AggregationResult(updates, statistics);
        }
    }

    private final Sampling sampling;
    private final Class<IN> in;
    private final MetricType out;

    @Override
    public long estimate(DateRange original) {
        final long size = sampling.getSize();

        if (size == 0)
            return -1;

        return original.rounded(size).diff() / size;
    }

    @Override
    public AggregationTraversal session(List<AggregationState> states, DateRange range) {
        final Set<Series> series = new HashSet<Series>();

        for (final AggregationState s : states)
            series.addAll(s.getSeries());

        final List<AggregationState> out = ImmutableList.of(new AggregationState(EMPTY, series));

        final long size = sampling.getSize();
        final DateRange rounded = range.rounded(size);

        final List<B> buckets = buildBuckets(rounded, size);
        return new AggregationTraversal(out, new Session(series, buckets, range.start(), size, sampling.getExtent()));
    }

    @Override
    public long extent() {
        return sampling.getExtent();
    }

    private List<B> buildBuckets(final DateRange range, long size) {
        final long start = range.start();
        final long count = range.diff() / size;

        if (count < 1 || count > MAX_BUCKET_COUNT)
            throw new IllegalArgumentException(String.format("range %s, size %d", range, size));

        final List<B> buckets = new ArrayList<>((int) count);

        for (int i = 0; i < count; i++) {
            buckets.add(buildBucket(start + size * i + size));
        }

        return buckets;
    }

    abstract protected B buildBucket(long timestamp);

    abstract protected Metric build(B bucket);
}
