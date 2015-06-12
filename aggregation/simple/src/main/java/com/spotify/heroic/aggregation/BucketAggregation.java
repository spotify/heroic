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

import lombok.Data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.simple.MaxBucket;
import com.spotify.heroic.aggregation.simple.MinBucket;
import com.spotify.heroic.aggregation.simple.SumBucket;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.Statistics;
import com.spotify.heroic.model.TimeData;

/**
 * A base aggregation that collects data in 'buckets', one for each sampled data point.
 *
 * A bucket aggregation is used to down-sample a lot of data into distinct buckets over time, making them useful for
 * presentation purposes. Buckets have to be thread safe.
 *
 * @author udoprog
 *
 * @param <IN>
 *            The expected input data type.
 * @param <OUT>
 *            The expected output data type.
 * @param <T>
 *            The bucket type.
 *
 * @see SumBucket
 * @see MaxBucket
 * @see MinBucket
 */
@Data
public abstract class BucketAggregation<IN extends TimeData, OUT extends TimeData, T extends Bucket<IN>> implements
        Aggregation {
    private final static Map<String, String> EMPTY = ImmutableMap.of();

    @Data
    private final class Session implements Aggregation.Session {
        private long sampleSize = 0;
        private long outOfBounds = 0;
        private long uselessScan = 0;

        private final List<T> buckets;
        private final long offset;
        private final long size;
        private final long extent;

        @SuppressWarnings("unchecked")
        @Override
        public void update(Group update) {
            long outOfBounds = 0;
            long sampleSize = 0;
            long uselessScan = 0;

            final List<IN> input = (List<IN>) update.getValues();

            for (final IN d : input) {
                ++sampleSize;

                if (!d.valid())
                    continue;

                final long first = d.getTimestamp();
                final long last = first + extent;

                for (long start = first; start < last; start += size) {
                    int i = (int) ((start - offset - 1) / size);

                    if (i < 0 || i >= buckets.size())
                        continue;

                    final Bucket<IN> bucket = buckets.get(i);

                    final long c = bucket.timestamp() - first;

                    // check that the current bucket is _within_ the extent.
                    if (!(c >= 0 && c <= extent))
                        continue;

                    bucket.update(update.getGroup(), d);
                }
            }

            synchronized (this) {
                this.outOfBounds += outOfBounds;
                this.sampleSize += sampleSize;
                this.uselessScan += uselessScan;
            }
        }

        @Override
        public Result result() {
            final List<OUT> result = new ArrayList<OUT>(buckets.size());

            for (final T bucket : buckets) {
                final OUT d = build(bucket);

                if (!d.valid())
                    continue;

                result.add(d);
            }

            final Statistics.Aggregator statistics = new Statistics.Aggregator(sampleSize, outOfBounds, uselessScan);
            final List<Group> updates = ImmutableList.of(new Group(Group.EMPTY_GROUP, result));
            return new Result(updates, statistics);
        }

        @Override
        public Class<?> output() {
            return output();
        }
    }

    private final Sampling sampling;
    private final Class<IN> in;
    private final Class<OUT> out;

    @Override
    public long estimate(DateRange original) {
        final long size = sampling.getSize();

        if (size == 0)
            return -1;

        return original.rounded(size).diff() / size;
    }

    @Override
    public Aggregation.Session session(Class<?> out, DateRange original) {
        if (!in.isAssignableFrom(out))
            throw new IllegalStateException("invalid input type [" + out + " => " + in + "]");

        final long size = sampling.getSize();
        final DateRange range = original.rounded(size);

        final List<T> buckets = buildBuckets(range, size);
        return new Session(buckets, range.start(), size, sampling.getExtent());
    }

    @Override
    public List<TraverseState> traverse(List<TraverseState> states) {
        final Set<Series> series = new HashSet<Series>();

        for (final TraverseState s : states)
            series.addAll(s.getSeries());

        return ImmutableList.of(new TraverseState(EMPTY, series));
    }

    @Override
    public Sampling sampling() {
        return sampling;
    }

    public Class<?> input() {
        return in;
    }

    public Class<?> output() {
        return out;
    }

    private List<T> buildBuckets(final DateRange range, long size) {
        final long start = range.start();
        final long count = range.diff() / size;

        final List<T> buckets = new ArrayList<T>((int) count);

        for (int i = 0; i < count; i++) {
            buckets.add(buildBucket(start + size * i + size));
        }

        return buckets;
    }

    abstract protected T buildBucket(long timestamp);

    abstract protected OUT build(T bucket);
}