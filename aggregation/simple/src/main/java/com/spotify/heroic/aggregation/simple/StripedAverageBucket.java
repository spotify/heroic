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
 * Bucket that calculates the average of all samples seen.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class StripedAverageBucket extends AbstractBucket implements DoubleBucket {
    private final long timestamp;

    private final DoubleAdder value = new DoubleAdder();
    private final LongAdder count = new LongAdder();

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void updatePoint(Map<String, String> tags, Point d) {
        value.add(d.getValue());
        count.increment();
    }

    @Override
    public void updateSpread(Map<String, String> tags, Spread sample) {
        value.add(sample.getSum());
        count.add(sample.getCount());
    }

    @Override
    public double value() {
        final long count = this.count.sum();

        if (count == 0) {
            return Double.NaN;
        }

        return value.sum() / count;
    }
}
