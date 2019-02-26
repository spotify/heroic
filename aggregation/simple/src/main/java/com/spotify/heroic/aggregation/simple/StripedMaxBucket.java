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

import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.Map;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.function.DoubleBinaryOperator;

/**
 * A bucket implementation that retains the largest (max) value seen.
 * <p>
 * This bucket uses primitives based on striped atomic updates to reduce contention across CPUs.
 *
 * @author udoprog
 */
public class StripedMaxBucket extends AbstractBucket implements DoubleBucket {
    private static final DoubleBinaryOperator maxFn = (left, right) -> Math.max(left, right);

    private final long timestamp;

    private final DoubleAccumulator max = new DoubleAccumulator(maxFn, Double.NEGATIVE_INFINITY);

    @java.beans.ConstructorProperties({ "timestamp" })
    public StripedMaxBucket(final long timestamp) {
        this.timestamp = timestamp;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void updatePoint(Map<String, String> key, Point d) {
        max.accumulate(d.getValue());
    }

    @Override
    public void updateSpread(Map<String, String> key, Spread d) {
        max.accumulate(d.getMax());
    }

    @Override
    public double value() {
        final double result = max.doubleValue();

        if (!Double.isFinite(result)) {
            return Double.NaN;
        }

        return result;
    }
}
