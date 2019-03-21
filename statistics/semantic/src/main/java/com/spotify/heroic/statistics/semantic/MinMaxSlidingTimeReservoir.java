/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.ToString;

public class MinMaxSlidingTimeReservoir implements Reservoir {
    // max spins until calling Thread.yield()
    private static final int MAX_SPINS = 4;

    private final ConcurrentSkipListMap<Long, AtomicReference<MinMaxEntry>> measurements =
        new ConcurrentSkipListMap<>();
    private final Clock clock;
    private final int size;
    private final long step;
    private final Reservoir delegate;

    /**
     * Build a new reservoir.
     *
     * @param clock Clock to use as a time source
     * @param size Number of buckets to maintain
     * @param step Step between each bucket
     * @param stepUnit Time unit used in 'step'
     * @param delegate Delegate reservoir that min/max is being corrected for.
     */
    public MinMaxSlidingTimeReservoir(
        final Clock clock, final int size, final long step, final TimeUnit stepUnit,
        final Reservoir delegate
    ) {
        this.clock = clock;
        this.size = size;
        this.step = stepUnit.toNanos(step);
        this.delegate = delegate;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void update(long value) {
        trimIfNeeded();

        final long offset = clock.getTick() / step;

        final AtomicReference<MinMaxEntry> reference = measurements.computeIfAbsent(offset,
            o -> new AtomicReference<>(new MinMaxEntry(value, value)));

        int spins = 0;

        while (true) {
            final MinMaxEntry old = reference.get();

            if (old.min <= value && value <= old.max) {
                break;
            }

            final MinMaxEntry newEntry =
                new MinMaxEntry(Math.min(old.min, value), Math.max(old.max, value));

            if (reference.compareAndSet(old, newEntry)) {
                break;
            }

            if (spins++ >= MAX_SPINS) {
                Thread.yield();
                spins = 0;
            }
        }

        delegate.update(value);
    }

    @Override
    public Snapshot getSnapshot() {
        trimIfNeeded();

        final long first = calculateFirstBucket();

        final Optional<MinMaxEntry> value = measurements
            .tailMap(first)
            .values()
            .stream()
            .map(AtomicReference::get)
            .reduce((a, b) -> new MinMaxEntry(Math.min(a.min, b.min), Math.max(a.max, b.max)));

        final Snapshot snapshot = delegate.getSnapshot();

        final Optional<Snapshot> slidingSnapshot = value.map(minMax -> {
            final long[] values = snapshot.getValues();

            if (values.length > 0) {
                values[0] = Math.min(minMax.min, values[0]);
                values[values.length - 1] = Math.max(minMax.max, values[values.length - 1]);
            }

            return new UniformSnapshot(values);
        });

        return slidingSnapshot.orElse(snapshot);
    }

    /**
     * Trim min-max entries before the first bucket.
     */
    private void trimIfNeeded() {
        final long first = calculateFirstBucket();

        final Map.Entry<Long, AtomicReference<MinMaxEntry>> firstEntry = measurements.firstEntry();

        if (firstEntry != null && firstEntry.getKey() <= first) {
            // removes all entries older than first
            measurements.headMap(first, true).clear();
        }
    }

    /**
     * Calculate the first possible bucket that is within the current time interval.
     */
    long calculateFirstBucket() {
        return (clock.getTick() - (step * size)) / step;
    }

    @ToString
    private static class MinMaxEntry {
        private final long min;
        private final long max;

        private MinMaxEntry(final long min, final long max) {
            this.min = min;
            this.max = max;
        }
    }
}
