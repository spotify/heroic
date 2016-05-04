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

import com.spotify.heroic.aggregation.AbstractAnyBucket;
import com.spotify.heroic.metric.Metric;
import lombok.RequiredArgsConstructor;

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
@RequiredArgsConstructor
public class CountUniqueBucket extends AbstractAnyBucket {
    private final long timestamp;

    private final AtomicInteger count = new AtomicInteger(0);
    private final Set<Integer> seen =
        Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(Map<String, String> tags, Metric d) {
        if (seen.add(tags.hashCode() ^ d.valueHash())) {
            count.incrementAndGet();
        }
    }

    public long count() {
        return count.get();
    }
}
