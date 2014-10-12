package com.spotify.heroic.aggregation.simple;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.model.DataPoint;

public class MaxBucketTest {
    @Test
    public void testInitialValue() {
        final MaxBucket b = new MaxBucket(0);
        Assert.assertEquals(Double.NaN, b.value(), 0.0);
    }

    @Test
    public void testMinValues() {
        final MaxBucket b = new MaxBucket(0);
        b.update(new DataPoint(0, 20.0));
        b.update(new DataPoint(0, 10.0));
        Assert.assertEquals(20.0, b.value(), 0.0);
    }
}
