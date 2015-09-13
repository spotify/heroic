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
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import lombok.RequiredArgsConstructor;

/**
 * A lock-free implementation for calculating the standard deviation over many values.
 *
 * This bucket uses primitives based on striped atomic updates to reduce contention across CPUs.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class StripedStdDevBucket extends AbstractBucket implements DoubleBucket {
    private final DoubleAdder sum = new DoubleAdder();
    private final DoubleAdder sum2 = new DoubleAdder();
    private final LongAdder count = new LongAdder();

    private final long timestamp;

    @Override
    public void updateSpread(Map<String, String> tags, Spread d) {
        sum.add(d.getSum());
        sum2.add(d.getSum2());
        count.add(d.getCount());
    }

    @Override
    public void updatePoint(Map<String, String> tags, Point d) {
        final double v = d.getValue();

        sum.add(v);
        sum2.add(v * v);
        count.increment();
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    public double value() {
        final long count = this.count.sum();

        if (count == 0) {
            return Double.NaN;
        }

        final double sum = this.sum.sum(), sum2 = this.sum2.sum();
        final double mean = sum / count;

        return Math.sqrt((sum2 / count) - (mean * mean));
    }
}
