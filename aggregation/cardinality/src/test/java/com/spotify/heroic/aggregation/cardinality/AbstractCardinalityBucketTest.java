package com.spotify.heroic.aggregation.cardinality;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.Point;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public abstract class AbstractCardinalityBucketTest {
    final Map<String, String> t1 = ImmutableMap.of("foo", "bar", "baz", "one");
    final Map<String, String> t2 = ImmutableMap.of("foo", "bar", "baz", "two");

    protected abstract CardinalityBucket setupBucket(long timestamp);

    protected double allowedError() {
        return 0.01D;
    }

    @Test
    public void basicTest() {
        final CardinalityBucket bucket = setupBucket(42);
        bucket.update(t1, new Point(42, 1D));
        bucket.update(t1, new Point(42, 1D));
        bucket.update(t1, new Point(42, 2D));
        bucket.update(t2, new Point(42, 1D));

        assertWithinVariance(3L, bucket.count());
    }

    @Test
    public void strainTest() {
        final CardinalityBucket bucket = setupBucket(42);

        for (int i = 0; i < 100000; i++) {
            bucket.update(t1, new Point(42, 1D * i));
            bucket.update(t2, new Point(42, 1D * i));
        }

        assertWithinVariance(200000L, bucket.count());
    }

    private void assertWithinVariance(final long expected, final long count) {
        final double allowedError = allowedError();
        final double error = Math.abs(((double) expected / (double) count) - 1.0D);

        assertTrue(String.format("Error (%f) not within (%f)", error, allowedError),
            error <= allowedError);
    }
}
