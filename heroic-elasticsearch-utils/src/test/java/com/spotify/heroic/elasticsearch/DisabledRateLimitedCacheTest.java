package com.spotify.heroic.elasticsearch;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DisabledRateLimitedCacheTest {
    private ConcurrentMap<String, Boolean> cache = new ConcurrentHashMap<>();

    @Mock
    Runnable notifier;

    @Test
    public void testCache() {
        final DisabledRateLimitedCache<String>  writeCache = new DisabledRateLimitedCache<>(cache);
        assertTrue(writeCache.acquire("key1", notifier));
        assertFalse(writeCache.acquire("key1", notifier));
        assertTrue(writeCache.acquire("key2", notifier));

        verify(notifier, never()).run();
    }


}
