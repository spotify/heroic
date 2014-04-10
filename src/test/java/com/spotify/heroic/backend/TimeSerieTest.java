package com.spotify.heroic.backend;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class TimeSerieTest {
    @Test
    public void testEquality() throws Exception {
        final TimeSerie a = new TimeSerie("foo", new HashMap<String, String>());
        final TimeSerie b = new TimeSerie("foo", new HashMap<String, String>());
        Assert.assertEquals(a, b);
    }

    @Test
    public void testHashCode() throws Exception {
        final TimeSerie a = new TimeSerie("foo", new HashMap<String, String>());
        final TimeSerie b = new TimeSerie("foo", new HashMap<String, String>());
        Assert.assertEquals(a.hashCode(), b.hashCode());

        Set<TimeSerie> series = new HashSet<TimeSerie>();
        series.add(a);
        series.add(b);

        Assert.assertEquals(1, series.size());
    }
}
