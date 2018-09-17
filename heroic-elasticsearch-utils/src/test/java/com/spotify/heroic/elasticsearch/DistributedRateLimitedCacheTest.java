package com.spotify.heroic.elasticsearch;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.RateLimiter;
import com.spotify.folsom.MemcacheClient;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DistributedRateLimitedCacheTest {
    private DistributedRateLimitedCache<String> writeCache;
    private ConcurrentMap<String, Boolean> cache;
    private String key1 = "key1";
    private int MEMCACHED_TTL_SECONDS = 60;

    @Mock
    RateLimiter rateLimiter;


    @Mock
    MemcacheClient memcacheClient;

    @Mock
    Runnable cacheHit;

    @Before
    public void setUp() {
       cache = new ConcurrentHashMap<>();
       writeCache = new DistributedRateLimitedCache<>(
         cache, rateLimiter, memcacheClient, MEMCACHED_TTL_SECONDS);
    }


    @Test
    public void localCacheHit(){
        cache.putIfAbsent("key1", true);

        assertFalse(writeCache.acquire("key1", cacheHit));

        verify(cacheHit, atLeastOnce()).run();
        verify(rateLimiter, never()).acquire();
    }

    @Test
    public void memcachedCacheHit() throws
                                   ExecutionException, InterruptedException, TimeoutException {
        doReturn(setupMockedMemcachedGet(key1)).when(memcacheClient).get(Matchers.anyString());
        assertFalse(writeCache.acquire(key1, cacheHit));

        verify(cacheHit, atLeastOnce()).run();
        verify(rateLimiter, never()).acquire();
    }

    @Test
    public void noCacheHit() throws InterruptedException, ExecutionException, TimeoutException {
        doReturn(setupMockedMemcachedGet(null)).when(memcacheClient).get(Matchers.anyString());
        doReturn(1D).when(rateLimiter).acquire();

        assertTrue(writeCache.acquire(key1, cacheHit));
        assertTrue(cache.get("key1"));
        assertEquals(1, writeCache.size());

        verify(cacheHit, never()).run();
        verify(rateLimiter, atLeastOnce()).acquire();
        verify(memcacheClient).set(
          Matchers.anyString(), Matchers.eq("true"), Matchers.eq(MEMCACHED_TTL_SECONDS));
    }


    public CompletionStage setupMockedMemcachedGet(final String key)
      throws InterruptedException, ExecutionException, TimeoutException {
        final CompletableFuture completableFuture = Mockito.mock(CompletableFuture.class);
        doReturn(key).when(completableFuture).get(Matchers.anyLong(), Matchers.anyObject());

        final CompletionStage completionStage = Mockito.mock(CompletionStage.class);
        doReturn(completableFuture).when(completionStage).toCompletableFuture();

        return completionStage;
    }
}
