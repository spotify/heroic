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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import lombok.RequiredArgsConstructor;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.RateLimiter;

/**
 * A cache that does not allow to be called more than a specific rate.
 *
 * @param <K>
 * @param <V>
 *
 * @author mehrdad
 */
@RequiredArgsConstructor
public class DefaultRateLimitedCache<K, V> implements RateLimitedCache<K, V> {
    private final Cache<K, V> cache;
    private final RateLimiter rateLimiter;

    public V get(K key, final Callable<V> callable)
            throws ExecutionException, RateLimitExceededException {
        final V shortcut = cache.getIfPresent(key);

        if (shortcut != null) {
            return shortcut;
        }

        if (!rateLimiter.tryAcquire()) {
            throw new RateLimitExceededException();
        }

        return cache.get(key, callable);
    }
}
