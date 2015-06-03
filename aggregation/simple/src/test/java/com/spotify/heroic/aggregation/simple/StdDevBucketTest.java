package com.spotify.heroic.aggregation.simple;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.spotify.heroic.model.DataPoint;

public class StdDevBucketTest {
    private static final Map<String, String> TAGS = new HashMap<>();

    @Test
    public void testSomething() {
        StdDevBucket b = new StdDevBucket(0);
        b.update(TAGS, new DataPoint(0, 0.0));
        b.update(TAGS, new DataPoint(0, 100.0));
    }
}
