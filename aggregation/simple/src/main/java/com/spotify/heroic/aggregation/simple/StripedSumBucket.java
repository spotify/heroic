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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.DoubleAdder;

import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import lombok.Data;

/**
 * Bucket that keeps track of the amount of data points seen, and there summed value.
 *
 * This bucket uses primitives based on striped atomic updates to reduce contention across CPUs.
 *
 * @author udoprog
 */
@Data
public class StripedSumBucket extends AbstractBucket implements DoubleBucket {
    private final long timestamp;
    /* the sum of all seen values */
    private final DoubleAdder sum = new DoubleAdder();
    /* if the sum is valid (e.g. has at least one value) */
    private final AtomicBoolean valid = new AtomicBoolean();

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void updatePoint(Map<String, String> tags, Point d) {
        sum.add(d.getValue());
        valid.compareAndSet(false, true);
    }

    @Override
    public void updateSpread(Map<String, String> tags, Spread d) {
        sum.add(d.getSum());
        valid.compareAndSet(false, true);
    }

    @Override
    public double value() {
        if (!valid.get())
            return Double.NaN;

        return sum.sum();
    }
}