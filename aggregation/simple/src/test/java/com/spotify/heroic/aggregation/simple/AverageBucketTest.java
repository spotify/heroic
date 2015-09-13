package com.spotify.heroic.aggregation.simple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;

public class AverageBucketTest {
    private static final Map<String, String> tags = ImmutableMap.of();

    public Collection<? extends DoubleBucket> buckets() {
        return ImmutableList.<DoubleBucket> of(new AverageBucket(0l), new StripedAverageBucket(0l));
    }

    @Test
    public void testZeroValue() {
        for (final DoubleBucket bucket : buckets()) {
            assertTrue(Double.isNaN(bucket.value()));
        }
    }

    @Test
    public void testAddSome() {
        for (final DoubleBucket bucket : buckets()) {
            bucket.updatePoint(tags, new Point(0, 10.0));
            bucket.updatePoint(tags, new Point(0, 20.0));
            assertEquals(bucket.getClass().getSimpleName(), 15.0, bucket.value(), 0.0);
        }
    }
}