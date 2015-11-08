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

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import lombok.RequiredArgsConstructor;

/**
 * A bucket implementation that retains the smallest (min) value seen.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class MinBucket extends AbstractBucket implements DoubleBucket {
    private final long timestamp;

    private final AtomicDouble value = new AtomicDouble(Double.POSITIVE_INFINITY);

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void updatePoint(Map<String, String> tags, Point d) {
        while (true) {
            double current = value.get();

            if (current < d.getValue()) {
                break;
            }

            if (value.compareAndSet(current, d.getValue())) {
                break;
            }
        }
    }

    @Override
    public void updateSpread(Map<String, String> tags, Spread d) {
        while (true) {
            double current = value.get();

            if (current < d.getMin()) {
                break;
            }

            if (value.compareAndSet(current, d.getMin())) {
                break;
            }
        }
    }

    @Override
    public double value() {
        final double result = value.get();

        if (!Double.isFinite(result)) {
            return Double.NaN;
        }

        return result;
    }
}
