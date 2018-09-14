package com.spotify.heroic.elasticsearch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.RateLimiter;
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
    RateLimiter rateLimiter;

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

        assertEquals(false, c.acquire(key, notifier));

        verify(cache).get(key);
        verify(rateLimiter, never()).tryAcquire();
        verify(cache, never()).putIfAbsent(key, true);
    }

    @Test
    public void testGet() throws ExecutionException, RateLimitExceededException {
        final DefaultRateLimitedCache<K> c = new DefaultRateLimitedCache<K>(cache, rateLimiter);
        doReturn(null).when(cache).get(key);
        doReturn(1D).when(rateLimiter).acquire();
        doReturn(null).when(cache).putIfAbsent(key, true);

        assertEquals(true, c.acquire(key, notifier));

        verify(cache).get(key);
        verify(rateLimiter).acquire();
        verify(cache).putIfAbsent(key, true);
    }

    public static interface K {
    }

    public static interface V {
    }
}
