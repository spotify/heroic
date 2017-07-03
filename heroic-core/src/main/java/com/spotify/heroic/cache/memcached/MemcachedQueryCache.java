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

package com.spotify.heroic.cache.memcached;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.spotify.folsom.MemcacheClient;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.cache.CacheScope;
import com.spotify.heroic.cache.QueryCache;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.metric.CacheInfo;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.ResultLimits;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.time.Clock;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ResolvableFuture;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@CacheScope
public class MemcachedQueryCache implements QueryCache {
    private static final QueryTrace.Identifier IDENTIFIER =
        QueryTrace.identifier(MemcachedQueryCache.class);

    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    private final Managed<MemcacheClient<byte[]>> client;
    private final ObjectMapper mapper;
    private final AsyncFramework async;
    private final Clock clock;
    private final Optional<Integer> maxTtlSeconds;

    @Inject
    public MemcachedQueryCache(
        final Managed<MemcacheClient<byte[]>> client,
        @Named(HeroicMappers.APPLICATION_JSON_INTERNAL) final ObjectMapper mapper,
        final AsyncFramework async, final Clock clock,
        @Named("maxTtl") final Optional<Duration> maxTtl
    ) {
        this.client = client;
        this.mapper = mapper;
        this.async = async;
        this.clock = clock;
        this.maxTtlSeconds = maxTtl.map(d -> (int) d.convert(TimeUnit.SECONDS));
    }

    @Override
    public AsyncFuture<QueryResult> load(
        FullQuery.Request request, Supplier<AsyncFuture<QueryResult>> loader
    ) {
        final long cadence = request.getAggregation().cadence();

        // can't cache aggregation results _without_ a cadence.
        if (cadence <= 0) {
            return loader.get();
        }

        // only cache if range is rounded to cadence.
        if (!request.getFeatures().hasFeature(Feature.SHIFT_RANGE)) {
            return loader.get();
        }

        // is caching permitted?
        if (!request.getFeatures().hasFeature(Feature.CACHE_QUERY)) {
            return loader.get();
        }

        final String key = buildCacheKey(request);

        final QueryTrace.NamedWatch watch =
            request.getOptions().tracing().watch(IDENTIFIER.extend(key));

        log.debug("{}: performing cache lookup", key);

        return client.doto(client -> {
            final ResolvableFuture<QueryResult> future = async.future();

            Futures.addCallback(client.get(key), new FutureCallback<byte[]>() {
                @Override
                public void onSuccess(@Nullable final byte[] result) {
                    if (result == null) {
                        cacheSet(future, loader, key, cadence);
                        return;
                    }

                    final CachedResult cachedResult;

                    try {
                        cachedResult = mapper.readValue(result, CachedResult.class);
                    } catch (final Exception e) {
                        log.error("{}: failed to deserialize value from cache", key, e);
                        // fallback to regular request
                        cacheSet(future, loader, key, cadence);
                        return;
                    }

                    final int ttl = calculateTtl(cadence);
                    final CacheInfo cache = new CacheInfo(true, ttl, key);

                    final QueryResult queryResult =
                        new QueryResult(cachedResult.getRange(), cachedResult.getGroups(),
                            ImmutableList.of(), watch.end(), cachedResult.getLimits(),
                            cachedResult.getPreAggregationSampleSize(), Optional.of(cache));

                    future.resolve(queryResult);
                }

                @Override
                public void onFailure(final Throwable t) {
                    log.error("{}: failed to load value from cache", key, t);
                    // fallback to regular request
                    cacheSet(future, loader, key, cadence);
                }
            });

            return future;
        });
    }

    private void cacheSet(
        final ResolvableFuture<QueryResult> future, final Supplier<AsyncFuture<QueryResult>> loader,
        final String key, final long cadence
    ) {
        log.debug("{}: loading value", key);

        loader.get().onDone(new FutureDone<QueryResult>() {
            @Override
            public void failed(final Throwable cause) throws Exception {
                future.fail(cause);
            }

            @Override
            public void resolved(final QueryResult result) throws Exception {
                final int ttl = calculateTtl(cadence);

                future.resolve(result.withCache(new CacheInfo(false, ttl, key)));

                // only store results if there are no errors
                // TODO: partial result caching for successful shards?
                if (result.getErrors().isEmpty()) {
                    storeResult(key, ttl, result);
                }
            }

            @Override
            public void cancelled() throws Exception {
                future.cancel();
            }
        });
    }

    /**
     * Store the result.
     *
     * @param key key to store under
     * @param ttl cadence of the queried data
     * @param queryResult query results
     */
    private void storeResult(final String key, final int ttl, final QueryResult queryResult) {
        if (ttl <= 0) {
            return;
        }

        final CachedResult cachedResult =
            new CachedResult(queryResult.getRange(), queryResult.getGroups(),
                queryResult.getPreAggregationSampleSize(), queryResult.getLimits());

        final byte[] bytes;

        try {
            bytes = mapper.writeValueAsBytes(cachedResult);
        } catch (final Exception e) {
            log.error("failed to serialize cached results", e);
            return;
        }

        final Borrowed<MemcacheClient<byte[]>> borrowed = client.borrow();

        if (borrowed.isValid()) {
            log.debug("{}: storing with ttl ({}s)", key, ttl);
            borrowed.get().set(key, bytes, ttl);
        }
    }

    private int calculateTtl(final long cadence) {
        final long timeInCadence = clock.currentTimeMillis() % cadence;
        final long timeUntilExpiry = cadence - timeInCadence;
        final int candidate =
            (int) TimeUnit.SECONDS.convert(timeUntilExpiry, TimeUnit.MILLISECONDS);
        return maxTtlSeconds.map(max -> Math.min(max, candidate)).orElse(candidate);
    }

    private String buildCacheKey(final FullQuery.Request request) {
        final Hasher hasher = HASH_FUNCTION.newHasher();
        request.hashTo(new ObjectHasher(hasher));
        return PREFIX + hasher.hash().toString();
    }

    /**
     * Only cache the relevant parts of the result.
     */
    @Data
    public static class CachedResult {
        private final DateRange range;
        private final List<ShardedResultGroup> groups;
        private final long preAggregationSampleSize;
        private final ResultLimits limits;
    }
}
