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

import com.google.common.base.Charsets;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.spotify.heroic.metric.Metric;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bucket that counts the number of seen events.
 *
 * @author udoprog
 */
public class ExactCardinalityBucket implements CardinalityBucket {
    private static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(128);
    private static final Ordering<String> KEY_ORDER = Ordering.from(String::compareTo);

    private final long timestamp;
    private final boolean includeKey;

    private final AtomicInteger count = new AtomicInteger(0);
    private final Set<HashCode> seen = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @java.beans.ConstructorProperties({ "timestamp", "includeKey" })
    public ExactCardinalityBucket(final long timestamp, final boolean includeKey) {
        this.timestamp = timestamp;
        this.includeKey = includeKey;
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

        if (seen.add(hasher.hash())) {
            count.incrementAndGet();
        }
    }

    @Override
    public long count() {
        return count.get();
    }

    @Override
    public byte[] state() {
        throw new RuntimeException("Bucket does not support state persisting");
    }
}
