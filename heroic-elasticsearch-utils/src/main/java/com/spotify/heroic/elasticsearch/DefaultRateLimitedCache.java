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

    public V get(K key, final Callable<V> callable) throws ExecutionException, RateLimitExceededException {
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