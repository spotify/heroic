package com.spotify.heroic.common;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SeriesTest {
    @Test
    public void testEquality() throws Exception {
        final Series a = Series.of("foo");
        final Series b = Series.of("foo");
        assertEquals(a, b);
    }

    @Test
    public void testHashCode() throws Exception {
        final Series a = Series.of("foo");
        final Series b = Series.of("foo");

        assertEquals(a.hash(), b.hash());
        assertEquals(a.hashCode(), b.hashCode());

        final Set<Series> series = new HashSet<Series>();
        series.add(a);
        series.add(b);

        assertEquals(1, series.size());
    }

    @Test
    public void toDSLTest() {
        final Series a = Series.of("system.cpu-user-perc",
            ImmutableMap.of("role", "heroicapi", "host", "heroicapi1.sto.spotify.net", "site",
                "sto"));

        assertEquals(
            "system.cpu-user-perc {host=heroicapi1.sto.spotify.net, role=heroicapi, site=sto}",
            a.toDSL());
    }
}
