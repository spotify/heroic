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

package com.spotify.heroic.elasticsearch;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.ConcurrentMap;

import lombok.RequiredArgsConstructor;

/**
 * A cache that does not allow to be called more than a specific rate.
 *
 * @param <K>
 * @param <V>
 *
 * @author mehrdad
 */
@RequiredArgsConstructor
public class DefaultRateLimitedCache<K> implements RateLimitedCache<K> {
    private final ConcurrentMap<K, Boolean> cache;
    private final RateLimiter rateLimiter;

    public boolean acquire(K key) {
        if (cache.get(key) != null) {
            return false;
        }

        if (!rateLimiter.tryAcquire()) {
            return false;
        }

        return cache.putIfAbsent(key, true) == null;
    }

    public int size() {
        return cache.size();
    }
}
