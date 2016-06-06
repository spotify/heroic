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

import com.google.common.hash.Hasher;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(exclude = {"valueHash"})
public class Spread implements Metric {
    private final long timestamp;
    private final long count;
    private final double sum;
    private final double sum2;
    private final double min;
    private final double max;

    public Spread(long timestamp, long count, double sum, double sum2, double min, double max) {
        this.timestamp = timestamp;
        this.count = count;
        this.sum = sum;
        this.sum2 = sum2;
        this.min = min;
        this.max = max;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean valid() {
        return true;
    }

    @Override
    public void hash(final Hasher hasher) {
        hasher.putInt(MetricType.SPREAD.ordinal());
        hasher.putLong(timestamp);
        hasher.putLong(count);
        hasher.putDouble(sum);
        hasher.putDouble(sum2);
        hasher.putDouble(min);
        hasher.putDouble(max);
    }
}
