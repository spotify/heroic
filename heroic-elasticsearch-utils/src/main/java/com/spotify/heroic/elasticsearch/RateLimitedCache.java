package com.spotify.heroic.elasticsearch;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;

public interface RateLimitedCache<K, V> {
    /**
     * Try to get a previously cached value, or load a new value from {@code callable} if the configured rate limited
     * permits it.
     *
     * @throws RateLimitExceededException
     *             If the configured rate is at capacity.
     * @see Cache#get(Object, Callable)
     */
    public V get(K key, final Callable<V> callable) throws ExecutionException, RateLimitExceededException;
}