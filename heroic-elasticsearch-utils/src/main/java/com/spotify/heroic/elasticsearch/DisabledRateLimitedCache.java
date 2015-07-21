package com.spotify.heroic.elasticsearch;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import lombok.RequiredArgsConstructor;

import com.google.common.cache.Cache;

@RequiredArgsConstructor
public class DisabledRateLimitedCache<K, V> implements RateLimitedCache<K, V> {
    private final Cache<K, V> cache;

    @Override
    public V get(K key, Callable<V> callable) throws ExecutionException, RateLimitExceededException {
        return cache.get(key, callable);
    }
}