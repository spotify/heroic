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

import java.util.concurrent.ConcurrentMap;

/**
 * A cache to reduce writes to Elasticsearch.
 *
 * Main difference from {@link DefaultRateLimitedCache} is there is no rate limiting enforced.
 */
public class DisabledRateLimitedCache<K> implements RateLimitedCache<K> {
    private final ConcurrentMap<K, Boolean> cache;

    @java.beans.ConstructorProperties({ "cache" })
    public DisabledRateLimitedCache(final ConcurrentMap<K, Boolean> cache) {
        this.cache = cache;
    }

    @Override
    public boolean acquire(K key, final Runnable cacheHit) {
        if (cache.putIfAbsent(key, true) != null) {
            cacheHit.run();
            return false;
        }
        return true;
    }

    @Override
    public int size() {
        return 0;
    }
}
