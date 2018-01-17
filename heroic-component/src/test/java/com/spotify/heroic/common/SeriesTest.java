package com.spotify.heroic.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
            "system.cpu-user-perc {host=heroicapi1.sto.spotify.net, role=heroicapi, site=sto} {}",
            a.toDSL());
    }

    @Test
    public void testCompareToTags() throws Exception {
        {
            final Series a = Series.of("foo", ImmutableMap.of("z", "s", "a", "d"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "d", "z", "s"));
            assertEquals(0, a.compareTo(b));
        }

        {
            final Series a = Series.of("foo", ImmutableMap.of("a", "d", "y", "s"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "d", "z", "s"));
            assertEquals(-1, a.compareTo(b));
        }

        {
            final Series a = Series.of("foo", ImmutableMap.of( "a", "e", "z", "s"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "d", "z", "s"));
            assertEquals(1, a.compareTo(b));
        }

        {
            final Series a = Series.of("foo", ImmutableMap.of( "a", "b"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "b", "z", "s"));
            assertEquals(-1, a.compareTo(b));
        }

        {
            final Series a = Series.of("foo", ImmutableMap.of( "a", "b", "z", "s"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "b"));
            assertEquals(1, a.compareTo(b));
        }
    }

    @Test
    public void testCompareToResourceIdentifiers() throws Exception {
        {
            final Series a = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "v"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "v"));
            assertEquals(0, a.compareTo(b));
        }

        {
            final Series a = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("a", "v"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "v"));
            assertTrue(a.compareTo(b) < 0);
        }

        {
            final Series a = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "z"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "v"));
            assertTrue(a.compareTo(b) > 0);
        }

        {
            final Series a = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "v"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "v", "z", "s"));
            assertTrue (a.compareTo(b) < 0);
        }

        {
            final Series a = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "v", "z", "a"));
            final Series b = Series.of("foo", ImmutableMap.of("a", "d"), ImmutableSortedMap.of("k", "v"));
            assertTrue(a.compareTo(b) > 0);
        }
    }
}
