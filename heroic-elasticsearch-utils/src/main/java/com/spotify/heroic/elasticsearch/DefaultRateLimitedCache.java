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
        final Callable<V> outer = new Callable<V>() {
            @Override
            public V call() throws Exception {
                boolean canWrite = rateLimiter.tryAcquire();

                if (!canWrite) {
                    throw new RateLimitExceededException();
                }

                return callable.call();
            }
        };

        try {
            return cache.get(key, outer);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RateLimitExceededException) {
                throw new RateLimitExceededException(e);
            }

            throw e;
        }
    }
}