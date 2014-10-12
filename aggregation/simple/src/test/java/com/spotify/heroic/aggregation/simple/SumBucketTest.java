package com.spotify.heroic.aggregation.simple;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.model.DataPoint;

public class SumBucketTest {
    @Test
    public void testDefault() {
        SumBucket b = new SumBucket(0);
        Assert.assertEquals(0.0, b.value(), 0.0);
        Assert.assertEquals(0, b.count());
    }

    @Test
    public void testBasic() {
        SumBucket b = new SumBucket(0);
        b.update(new DataPoint(0, 10.0));
        b.update(new DataPoint(0, 20.0));
        Assert.assertEquals(30.0, b.value(), 0.0);
        Assert.assertEquals(2, b.count(), 0.0);
    }
}
