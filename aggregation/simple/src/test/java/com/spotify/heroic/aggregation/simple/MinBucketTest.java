package com.spotify.heroic.aggregation.simple;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.metric.Point;

public class MinBucketTest {
    private static final Map<String, String> TAGS = new HashMap<>();

    @Test
    public void testInitialValue() {
        final StripedMinBucket b = new StripedMinBucket(0);
        Assert.assertEquals(Double.NaN, b.value(), 0.0);
    }

    @Test
    public void testMinValues() {
        final StripedMinBucket b = new StripedMinBucket(0);
        b.updatePoint(TAGS, new Point(0, 10.0));
        b.updatePoint(TAGS, new Point(0, 20.0));
        Assert.assertEquals(10.0, b.value(), 0.0);
    }
}
