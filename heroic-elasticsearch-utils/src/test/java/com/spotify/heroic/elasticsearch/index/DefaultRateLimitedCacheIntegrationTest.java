package com.spotify.heroic.elasticsearch.index;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.heroic.elasticsearch.RateLimitExceededException;
import com.spotify.heroic.elasticsearch.DefaultRateLimitedCache;

@RunWith(MockitoJUnitRunner.class)
public class DefaultRateLimitedCacheIntegrationTest {
    @Mock
    Cache<K, V> cache;

    @Mock
    V shortcut;

    @Mock
    V value;

    @Mock
    K key;

    @Mock
    Callable<V> callable;

    @Test(timeout = 12000)
    public void testRateLimiting() throws Exception {
        doReturn(value).when(callable).call();

        final long runtime = 10000;

        final long allowedQps = 10;
        final long qps = 100;
        final long sleep = 1000 / qps;
        final long count = (runtime / sleep);

        long rateViolations = 0;
        long expectedViolations = (qps - allowedQps) * (runtime / 1000);

        final RateLimiter rateLimiter = RateLimiter.create((double) allowedQps);
        final DefaultRateLimitedCache<K, V> c = new DefaultRateLimitedCache<>(cache, rateLimiter);

        // will run for approximately 10 seconds (10ms * 1000), which generates a rate of 100qps.
        // after which ~990 requests (100qps * 10s - 1qps * 10s) should have been rejected.
        for (int i = 0; i < count; i++) {
            Thread.sleep(sleep);

            try {
                c.get(key, callable);
            } catch (RateLimitExceededException e) {
                rateViolations++;
            }
        }

        // allow for a 0.5% margin of error.
        double margin = 1.0 - ((double) rateViolations / (double) expectedViolations);
        assertTrue(String.format("expected a margin smaller than %.3f", margin), margin < 0.005);
    }

    public static interface K {
    }

    public static interface V {
    }
}