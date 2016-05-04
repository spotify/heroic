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
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Data
@EqualsAndHashCode(callSuper = true)
public class AverageBucket extends AbstractBucket implements DoubleBucket {
    private final long timestamp;
    private final AtomicDouble value = new AtomicDouble();
    private final AtomicLong count = new AtomicLong();

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void updatePoint(Map<String, String> tags, Point d) {
        value.addAndGet(d.getValue());
        count.incrementAndGet();
    }

    @Override
    public void updateSpread(Map<String, String> tags, Spread sample) {
        value.addAndGet(sample.getSum());
        count.addAndGet(sample.getCount());
    }

    @Override
    public double value() {
        final long count = this.count.get();

        if (count == 0) {
            return Double.NaN;
        }

        return value.get() / count;
    }
}
