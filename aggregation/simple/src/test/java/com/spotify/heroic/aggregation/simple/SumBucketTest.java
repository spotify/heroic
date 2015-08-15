package com.spotify.heroic.aggregation.simple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;

public class SumBucketTest {
    private static final Map<String, String> tags = ImmutableMap.of();

    public Collection<? extends DoubleBucket<Point>> buckets() {
        return ImmutableList.<DoubleBucket<Point>> of(new SumBucket(0l), new StripedSumBucket(0l));
    }

    @Test
    public void testZeroValue() {
        for (final DoubleBucket<Point> bucket : buckets()) {
            assertTrue(bucket.getClass().getSimpleName(), Double.isNaN(bucket.value()));
        }
    }

    @Test
    public void testAddSome() {
        for (final DoubleBucket<Point> bucket : buckets()) {
            bucket.update(tags, MetricType.POINT, new Point(0, 10.0));
            bucket.update(tags, MetricType.POINT, new Point(0, 20.0));
            assertEquals(bucket.getClass().getSimpleName(), 30.0, bucket.value(), 0.0);
        }
    }
}
