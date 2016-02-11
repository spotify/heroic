package com.spotify.heroic.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class SeriesTest {
    @Test
    public void testEquality() throws Exception {
        final Series a = Series.of("foo");
        final Series b = Series.of("foo");
        Assert.assertEquals(a, b);
    }

    @Test
    public void testHashCode() throws Exception {
        final Series a = Series.of("foo");
        final Series b = Series.of("foo");

        Assert.assertEquals(a.hash(), b.hash());
        Assert.assertEquals(a.hashCode(), b.hashCode());

        final Set<Series> series = new HashSet<Series>();
        series.add(a);
        series.add(b);

        Assert.assertEquals(1, series.size());
    }
}
