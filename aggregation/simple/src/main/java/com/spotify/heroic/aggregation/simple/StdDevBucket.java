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
import java.util.concurrent.atomic.AtomicReference;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;

/**
 * Bucket that calculates the standard deviation of all buckets seen.
 *
 * This uses Welford's method, as presented in http://www.johndcook.com/blog/standard_deviation/
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class StdDevBucket extends AbstractBucket implements DoubleBucket {
    private static final Cell ZERO = new Cell(0.0, 0.0, 0);

    private final long timestamp;
    private AtomicReference<Cell> cell = new AtomicReference<>(ZERO);

    @Override
    public void updatePoint(Map<String, String> tags, Point d) {
        final double value = d.getValue();

        while (true) {
            final Cell c = cell.get();

            final long count = c.count + 1;
            final double delta = value - c.mean;
            final double mean = c.mean + delta / count;
            final double s = c.s + delta * (value - mean);

            final Cell n = new Cell(mean, s, count);

            if (cell.compareAndSet(c, n)) {
                break;
            }
        }
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public double value() {
        final Cell c = cell.get();

        if (c.count <= 1) {
            return Double.NaN;
        }

        return Math.sqrt(c.s / (c.count - 1));
    }

    @RequiredArgsConstructor
    private static class Cell {
        private final double mean;
        private final double s;
        private final long count;
    }
}