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

package com.spotify.heroic.metric;

import java.util.Comparator;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(exclude = { "valueHash" })
public class Spread implements Metric {
    private final long timestamp;
    private final long count;
    private final double sum;
    private final double sum2;
    private final double min;
    private final double max;
    private final int valueHash;

    public Spread(long timestamp, long count, double sum, double sum2, double min, double max) {
        this.timestamp = timestamp;
        this.count = count;
        this.sum = sum;
        this.sum2 = sum2;
        this.min = min;
        this.max = max;
        this.valueHash = calculateValueHash(count, sum, sum2, min, max);
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int valueHash() {
        return valueHash;
    }

    @Override
    public boolean valid() {
        return true;
    }

    static int calculateValueHash(long count, double sum, double sum2, double min, double max) {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (count ^ (count >>> 32));
        long temp;
        temp = Double.doubleToLongBits(max);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(min);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(sum);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(sum2);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public static Comparator<Metric> comparator() {
        return comparator;
    }

    static final Comparator<Metric> comparator = new Comparator<Metric>() {
        @Override
        public int compare(Metric a, Metric b) {
            return Long.compare(a.getTimestamp(), b.getTimestamp());
        }
    };
}