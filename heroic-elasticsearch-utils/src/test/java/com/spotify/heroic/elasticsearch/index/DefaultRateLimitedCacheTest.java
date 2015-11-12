package com.spotify.heroic.elasticsearch.index;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.heroic.elasticsearch.RateLimitExceededException;
import com.spotify.heroic.elasticsearch.DefaultRateLimitedCache;

@RunWith(MockitoJUnitRunner.class)
public class DefaultRateLimitedCacheTest {
    @Mock
    Cache<K, V> cache;

    @Mock
    RateLimiter rateLimiter;

    @Mock
    V shortcut;

    @Mock
    V value;

    @Mock
    K key;

    @Mock
    Callable<V> callable;

    @Test
    public void testCacheEarlyHit() throws ExecutionException, RateLimitExceededException {
        final DefaultRateLimitedCache<K, V> c =
                new DefaultRateLimitedCache<K, V>(cache, rateLimiter);
        doReturn(shortcut).when(cache).getIfPresent(key);

        assertEquals(shortcut, c.get(key, callable));

        verify(cache).getIfPresent(key);
        verify(rateLimiter, never()).tryAcquire();
        verify(cache, never()).get(key, callable);
    }

    @Test(expected = RateLimitExceededException.class)
    public void testGetRateLimiting() throws ExecutionException, RateLimitExceededException {
        final DefaultRateLimitedCache<K, V> c =
                new DefaultRateLimitedCache<K, V>(cache, rateLimiter);
        doReturn(null).when(cache).getIfPresent(key);
        doReturn(false).when(rateLimiter).tryAcquire();

        c.get(key, callable);
    }

    @Test
    public void testGet() throws ExecutionException, RateLimitExceededException {
        final DefaultRateLimitedCache<K, V> c =
                new DefaultRateLimitedCache<K, V>(cache, rateLimiter);
        doReturn(null).when(cache).getIfPresent(key);
        doReturn(true).when(rateLimiter).tryAcquire();
        doReturn(value).when(cache).get(key, callable);

        assertEquals(value, c.get(key, callable));

        verify(cache).getIfPresent(key);
        verify(rateLimiter).tryAcquire();
        verify(cache).get(key, callable);
    }

    public static interface K {
    }

    public static interface V {
    }
}
