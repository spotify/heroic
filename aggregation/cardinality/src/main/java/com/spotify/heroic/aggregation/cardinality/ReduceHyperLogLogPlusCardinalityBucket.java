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

package com.spotify.heroic.aggregation.cardinality;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.Payload;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Bucket that counts the number of seen events.
 *
 * @author udoprog
 */
public class ReduceHyperLogLogPlusCardinalityBucket implements CardinalityBucket {
    private final long timestamp;

    private final ConcurrentLinkedQueue<HyperLogLogPlus> states = new ConcurrentLinkedQueue<>();

    public ReduceHyperLogLogPlusCardinalityBucket(final long timestamp) {
        this.timestamp = timestamp;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void updatePayload(
        final Map<String, String> key, final Payload sample
    ) {
        try {
            states.add(HyperLogLogPlus.Builder.build(sample.state()));
        } catch (final IOException e) {
            throw new RuntimeException("Failed to deserialize state", e);
        }
    }

    @Override
    public void update(final Map<String, String> key, final Metric d) {
    }

    @Override
    public long count() {
        final Iterator<HyperLogLogPlus> it = states.iterator();

        if (!it.hasNext()) {
            return 0L;
        }

        HyperLogLogPlus current = it.next();

        while (it.hasNext()) {
            try {
                current.addAll(it.next());
            } catch (CardinalityMergeException e) {
                throw new RuntimeException(e);
            }
        }

        return current.cardinality();
    }
}
