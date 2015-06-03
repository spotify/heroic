package com.spotify.heroic.aggregation.simple;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.model.DataPoint;

public class MaxBucketTest {
    private static final Map<String, String> TAGS = new HashMap<>();

    @Test
    public void testInitialValue() {
        final MaxBucket b = new MaxBucket(0);
        Assert.assertEquals(Double.NaN, b.value(), 0.0);
    }

    @Test
    public void testMinValues() {
        final MaxBucket b = new MaxBucket(0);
        b.update(TAGS, new DataPoint(0, 20.0));
        b.update(TAGS, new DataPoint(0, 10.0));
        Assert.assertEquals(20.0, b.value(), 0.0);
    }
}
