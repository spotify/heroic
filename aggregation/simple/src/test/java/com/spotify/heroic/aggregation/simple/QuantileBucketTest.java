package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.Point;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QuantileBucketTest {
    private static final Map<String, String> TAGS = new HashMap<>();
    private static final double ERROR = 0.01;

    @Test
    public void testCount() throws IOException {
        final QuantileBucket b = new QuantileBucket(0, 0.5, ERROR);
        b.updatePoint(TAGS, new Point(0, 1337.0));
        Assert.assertEquals(1337.0, b.value(), 0.0);
    }

    @Test
    public void testQuantiles() throws IOException {
        final QuantileBucket b = new QuantileBucket(0, 0.5, ERROR);

        for (int i = 1; i <= 10000; i++) {
            b.updatePoint(TAGS, new Point(0, i));
        }

        Assert.assertEquals(5000.0, b.value(), 10000 * ERROR);
    }

    @Test
    public void testQuantiles2() throws IOException {
        final QuantileBucket b = new QuantileBucket(0, 0.1, ERROR);

        for (int i = 1; i <= 10000; i++) {
            b.updatePoint(TAGS, new Point(0, i));
        }

        Assert.assertEquals(1000.0, b.value(), 10000 * ERROR);
    }
}
