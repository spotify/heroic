package com.spotify.heroic.aggregation.simple;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;

public class StdDevBucketTest {
    public Collection<? extends DoubleBucket> buckets() {
        return ImmutableList.<DoubleBucket> of(new StdDevBucket(0l), new StripedStdDevBucket(0l));
    }

    @Test
    public void testExpectedValues() {
        final Random rnd = new Random();

        final Map<String, String> tags = ImmutableMap.of();

        for (final DoubleBucket bucket : buckets()) {
            for (int i = 0; i < 1000; i++) {
                bucket.updatePoint(tags, new Point(0l, rnd.nextDouble()));
            }

            final double value = bucket.value();
            assertTrue(bucket.getClass().getSimpleName(), value <= 1.0d && value >= 0.0d);
        }
    }

    @Test
    public void testNaNOnZero() {
        final Map<String, String> tags = ImmutableMap.of();

        for (final DoubleBucket bucket : buckets()) {
            assertTrue(Double.isNaN(bucket.value()));
        }

        for (final DoubleBucket bucket : buckets()) {
            bucket.updatePoint(tags, new Point(0l, 0.0d));
            bucket.updatePoint(tags, new Point(0l, 0.0d));
            assertFalse(bucket.getClass().getSimpleName(), Double.isNaN(bucket.value()));
        }
    }
}