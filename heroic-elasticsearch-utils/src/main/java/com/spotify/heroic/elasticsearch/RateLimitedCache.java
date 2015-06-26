package com.spotify.heroic.elasticsearch;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import lombok.RequiredArgsConstructor;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.RateLimiter;

/**
 * A cache that does not allow to be called more than a specific rate
 * @author mehrdad
 *
 * @param <K>
 * @param <V>
 */
@RequiredArgsConstructor
public class RateLimitedCache<K, V> {

    private final Cache<K, V> cache;
    private final RateLimiter rateLimiter;

    public V get(K key, final Callable<V> callable) throws ExecutionException, RateLimitExceededException {
        System.out.println(cache.size());
        Callable<V> outer = new Callable<V>() {

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
