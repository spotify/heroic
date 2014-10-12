package com.spotify.heroic.aggregation.simple;

import org.junit.Test;

import com.spotify.heroic.model.DataPoint;

public class StdDevBucketTest {
    @Test
    public void testSomething() {
        StdDevBucket b = new StdDevBucket(0);
        b.update(new DataPoint(0, 0.0));
        b.update(new DataPoint(0, 100.0));
    }
}
