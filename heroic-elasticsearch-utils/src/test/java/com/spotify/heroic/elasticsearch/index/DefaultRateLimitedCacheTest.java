package com.spotify.heroic.elasticsearch.index;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.spotify.heroic.elasticsearch.DefaultRateLimitedCache;
import com.spotify.heroic.elasticsearch.RateLimitExceededException;
import com.spotify.heroic.elasticsearch.SimpleRateLimiter;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultRateLimitedCacheTest {
    @Mock
    ConcurrentMap<K, Boolean> cache;

    @Mock
    SimpleRateLimiter rateLimiter;

    final Boolean value = true;

    @Mock
    K key;

    @Mock
    Callable<V> callable;

    @Mock
    Runnable notifier;

    @Test
    public void testCacheEarlyHit() throws ExecutionException, RateLimitExceededException {
        final DefaultRateLimitedCache<K> c = new DefaultRateLimitedCache<K>(cache, rateLimiter);
        doReturn(value).when(cache).get(key);

        assertEquals(false, c.acquire(key, notifier, notifier));

        verify(cache).get(key);
        verify(rateLimiter, never()).tryAcquire();
        verify(cache, never()).putIfAbsent(key, true);
    }

    @Test
    public void testGetRateLimiting() throws ExecutionException, RateLimitExceededException {
        final DefaultRateLimitedCache<K> c = new DefaultRateLimitedCache<K>(cache, rateLimiter);
        doReturn(null).when(cache).get(key);
        doReturn(false).when(rateLimiter).tryAcquire();

        assertEquals(false, c.acquire(key, notifier, notifier));

        verify(cache).get(key);
        verify(rateLimiter).tryAcquire();
        verify(cache, never()).putIfAbsent(key, true);
    }

    @Test
    public void testGet() throws ExecutionException, RateLimitExceededException {
        final DefaultRateLimitedCache<K> c = new DefaultRateLimitedCache<K>(cache, rateLimiter);
        doReturn(null).when(cache).get(key);
        doReturn(true).when(rateLimiter).tryAcquire();
        doReturn(null).when(cache).putIfAbsent(key, true);

        assertEquals(true, c.acquire(key, notifier, notifier));

        verify(cache).get(key);
        verify(rateLimiter).tryAcquire();
        verify(cache).putIfAbsent(key, true);
    }

    public static interface K {
    }

    public static interface V {
    }
}
