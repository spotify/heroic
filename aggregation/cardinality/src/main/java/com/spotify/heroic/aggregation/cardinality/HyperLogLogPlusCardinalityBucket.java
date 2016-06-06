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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Charsets;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.spotify.heroic.metric.Metric;

import java.io.IOException;
import java.util.Map;

/**
 * Bucket that counts the number of seen events.
 *
 * @author udoprog
 */
public class HyperLogLogPlusCardinalityBucket implements CardinalityBucket {
    private static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(128);
    private static final Ordering<String> KEY_ORDER = Ordering.from(String::compareTo);

    private final long timestamp;
    private final boolean includeKey;
    private final HyperLogLogPlus seen;

    public HyperLogLogPlusCardinalityBucket(
        final long timestamp, final boolean includeKey, final int precision
    ) {
        this.timestamp = timestamp;
        this.includeKey = includeKey;
        this.seen = new HyperLogLogPlus(precision);
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(final Map<String, String> key, final Metric d) {
        final Hasher hasher = HASH_FUNCTION.newHasher();

        if (includeKey) {
            for (final String k : KEY_ORDER.sortedCopy(key.keySet())) {
                hasher.putString(k, Charsets.UTF_8).putString(key.get(k), Charsets.UTF_8);
            }
        }

        d.hash(hasher);

        final long hash = hasher.hash().asLong();
        seen.offerHashed(hash);
    }

    @Override
    public long count() {
        return seen.cardinality();
    }

    public byte[] state() {
        try {
            return seen.getBytes();
        } catch (final IOException e) {
            throw new RuntimeException("Could not persist state", e);
        }
    }
}
