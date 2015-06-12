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

package com.spotify.heroic.aggregation.simple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Data;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Spread;

/**
 * Bucket that keeps track of the amount of data points seen, and there summed value.
 *
 * Take care to not blindly trust {@link #value()} since it is initialized to 0 for simplicity. Always check
 * {@link #count()}, which if zero indicates that the {@link #value()} is undefined (e.g. NaN).
 *
 * @author udoprog
 */
@Data
public class SpreadBucket implements Bucket<DataPoint> {
    private final long timestamp;

    private final AtomicLong count = new AtomicLong(0);
    private final AtomicDouble sum = new AtomicDouble(0);

    private final AtomicDouble min = new AtomicDouble(Double.NaN);
    private final AtomicDouble max = new AtomicDouble(Double.NaN);

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(Map<String, String> tags, DataPoint d) {
        final double value = d.getValue();

        if (Double.isNaN(value))
            return;

        count.incrementAndGet();
        sum.addAndGet(value);

        updateMin(value);
        updateMax(value);
    }

    private void updateMin(final double value) {
        while (true) {
            final double current = min.get();

            if (!Double.isNaN(current) && value >= current)
                return;

            if (min.compareAndSet(current, value))
                return;
        }
    }

    private void updateMax(final double value) {
        while (true) {
            final double current = max.get();

            if (!Double.isNaN(current) && value <= current)
                return;

            if (max.compareAndSet(current, value))
                return;
        }
    }

    public long count() {
        return count.get();
    }

    public Spread newSpread() {
        return new Spread(timestamp, count.get(), sum.get(), min.get(), max.get());
    }
}
