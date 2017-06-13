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

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import lombok.RequiredArgsConstructor;

import java.util.Map;

/**
 * Bucket that keeps track of the amount of data points seen, and their squared values summed.
 * <p>
 * Take care to not blindly trust {@link #value()} since it is initialized to 0 for simplicity.
 * Always check {@link #count()}, which if zero indicates that the {@link #value()} is undefined
 * (e.g. NaN).
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class Sum2Bucket extends AbstractBucket implements DoubleBucket {
    private final long timestamp;

    /* the sum of seen values */
    private final AtomicDouble sum2 = new AtomicDouble();
    /* if the sum is valid (e.g. has at least one value) */
    private volatile boolean valid = false;

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void updatePoint(Map<String, String> key, Point d) {
        valid = true;
        sum2.addAndGet(d.getValue() * d.getValue());
    }

    @Override
    public void updateSpread(Map<String, String> key, Spread d) {
        valid = true;
        sum2.addAndGet(d.getSum2());
    }

    @Override
    public double value() {
        if (!valid) {
            return Double.NaN;
        }
        return sum2.get();
    }
}
