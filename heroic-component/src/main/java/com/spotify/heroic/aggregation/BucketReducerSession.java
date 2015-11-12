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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BucketReducerSession<B extends Bucket> implements ReducerSession {
    public static final long MAX_BUCKET_COUNT = 100000L;

    private final MetricType out;
    private final long offset;
    private final long size;
    private final long extent;
    private final List<B> buckets;
    private final Function<B, Metric> bucketConverter;

    private final LongAdder sampleSize = new LongAdder();

    public BucketReducerSession(MetricType out, long size, long extent,
            Function<Long, B> bucketBuilder, Function<B, Metric> bucketConverter, DateRange range) {
        this.out = out;
        this.offset = range.start();
        this.size = size;
        this.extent = extent;
        this.buckets = buildBuckets(range, size, bucketBuilder);
        this.bucketConverter = bucketConverter;
    }

    @Override
    public void updatePoints(Map<String, String> group, List<Point> values) {
        feed(MetricType.POINT, values, (bucket, m) -> bucket.updatePoint(group, m));
    }

    @Override
    public void updateEvents(Map<String, String> group, List<Event> values) {
        feed(MetricType.EVENT, values, (bucket, m) -> bucket.updateEvent(group, m));
    }

    @Override
    public void updateSpreads(Map<String, String> group, List<Spread> values) {
        feed(MetricType.SPREAD, values, (bucket, m) -> bucket.updateSpread(group, m));
    }

    @Override
    public void updateGroup(Map<String, String> group, List<MetricGroup> values) {
        feed(MetricType.GROUP, values, (bucket, m) -> bucket.updateGroup(group, m));
    }

    private <T extends Metric> void feed(final MetricType type, List<T> values,
            final BucketConsumer<B, T> consumer) {
        int sampleSize = 0;

        for (final T m : values) {
            if (!m.valid()) {
                continue;
            }

            final Iterator<B> buckets = matching(m);

            while (buckets.hasNext()) {
                consumer.apply(buckets.next(), m);
            }

            sampleSize += 1;
        }

        this.sampleSize.add(sampleSize);
    }

    private Iterator<B> matching(final Metric m) {
        // XXX: range should be `<expr> - 1` to match old behavior, change back?

        final long ts = m.getTimestamp() - offset;
        final long te = ts + extent;

        if (te < 0) {
            return Collections.emptyIterator();
        }

        // iterator that iterates from the largest to the smallest matching bucket for _this_
        // metric.
        return new Iterator<B>() {
            long current = te;

            @Override
            public boolean hasNext() {
                while ((current / size) >= buckets.size()) {
                    current -= size;
                }

                final long m = current % size;
                return (current >= 0 && current > ts) && (m >= 0 && m < extent);
            }

            @Override
            public B next() {
                final int index = (int) (current / size);
                current -= size;
                return buckets.get(index);
            }
        };
    }

    @Override
    public ReducerResult result() {
        final List<Metric> result = new ArrayList<>(buckets.size());

        for (final B bucket : buckets) {
            final Metric d = bucketConverter.apply(bucket);

            if (!d.valid()) {
                continue;
            }

            result.add(d);
        }

        final MetricCollection metrics = MetricCollection.build(out, result);
        final Statistics statistics =
                new Statistics(ImmutableMap.of(AggregationInstance.SAMPLE_SIZE, sampleSize.sum()));
        return new ReducerResult(ImmutableList.of(metrics), statistics);
    }

    private List<B> buildBuckets(final DateRange range, long size, Function<Long, B> builder) {
        final long start = range.start();
        final long count = (range.diff() + size) / size;

        if (count < 1 || count > MAX_BUCKET_COUNT) {
            throw new IllegalArgumentException(String.format("range %s, size %d", range, size));
        }

        final List<B> buckets = new ArrayList<>((int) count);

        for (int i = 0; i < count; i++) {
            buckets.add(builder.apply(start + size * i));
        }

        return buckets;
    }
}
