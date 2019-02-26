/*
 * Copyright (c) 2018 Spotify AB.
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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.folsom.MemcacheClient;
import com.spotify.heroic.statistics.MemcachedReporter;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * A distributed cache that does not allow to be called more than a specific rate.
 *
 * Instead of having a cache that is local to each consumer this allows the cache
 * to be distributed. This became necessary when migrating from Kafka -> Google PubSub as
 * a consumer with PubSub will not have a stable partition of metrics.
 *
 * @author dmichel
 */
@Slf4j
public class DistributedRateLimitedCache<K> implements RateLimitedCache<K> {

  private final ConcurrentMap<K, Boolean> cache;
  private final RateLimiter rateLimiter;
  private final MemcacheClient memcachedClient;
  private final int memcachedTtlSeconds;
  private final MemcachedReporter memcachedReporter;

  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();
  private final Tracer tracer = Tracing.getTracer();

  @java.beans.ConstructorProperties({ "cache", "rateLimiter", "memcachedClient",
                                      "memcachedTtlSeconds", "memcachedReporter" })
  public DistributedRateLimitedCache(final ConcurrentMap<K, Boolean> cache,
                                     final RateLimiter rateLimiter,
                                     final MemcacheClient memcachedClient,
                                     final int memcachedTtlSeconds,
                                     final MemcachedReporter memcachedReporter) {
    this.cache = cache;
    this.rateLimiter = rateLimiter;
    this.memcachedClient = memcachedClient;
    this.memcachedTtlSeconds = memcachedTtlSeconds;
    this.memcachedReporter = memcachedReporter;
  }


  /**
   *
   * @param key key to store/lookup in cache.
   * @param cacheHit function to call when the cache is hit (usually a metric reporter)
   * @return true - write to backend, false - do not write to backend
   */
  public boolean acquire(K key, final Runnable cacheHit) {
    try (Scope ss = tracer.spanBuilder("DistributedRateLimitedCache").startScopedSpan()) {
      Span span = tracer.getCurrentSpan();

      if (cache.get(key) != null) {
        span.addAnnotation("Found key in cache");
        cacheHit.run();
        return false;
      }

      final String cacheKey = buildKey(key);
      try {
        if (memcachedClient.get(
          cacheKey).toCompletableFuture().get(100, TimeUnit.MILLISECONDS) != null) {
          memcachedReporter.reportMemcachedHit();
          span.addAnnotation("Found key in memcached");
          cache.putIfAbsent(key, true);
          cacheHit.run();
          return false;
        } else {
          memcachedReporter.reportMemcachedMiss();
        }
      } catch (TimeoutException e) {
        span.setStatus(Status.INTERNAL.withDescription(e.getMessage()));
        memcachedReporter.reportMemcachedTimeout();
        log.debug("Failed to get key from memecached");
      } catch (InterruptedException | ExecutionException e) {
        span.setStatus(Status.INTERNAL.withDescription(e.getMessage()));
        memcachedReporter.reportMemcachedError();
        log.error("Failed to get key from memecached", e);
      }
      span.addAnnotation("Acquiring rate limiter");
      rateLimiter.acquire();
      span.addAnnotation("Acquired rate limiter");

      if (cache.putIfAbsent(key, true) != null) {
        cacheHit.run();
        return false;
      } else {
        span.addAnnotation("Add key in memcached " + cacheKey);
        memcachedClient.set(cacheKey, "true", memcachedTtlSeconds);
      }

      return true;
    }
  }

  private String buildKey(final K key) {
    final Hasher hasher = HASH_FUNCTION.newHasher();
    return hasher.putString(key.toString(), Charset.defaultCharset()).hash().toString();
  }

  public int size() {
    return cache.size();
  }
}
