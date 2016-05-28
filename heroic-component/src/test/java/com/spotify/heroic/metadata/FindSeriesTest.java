package com.spotify.heroic.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.spotify.heroic.AbstractReducedResultTest;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FindSeriesTest extends AbstractReducedResultTest {
    final Series a = Series.of("foo");
    final Series b = Series.of("bar");
    final Series c = Series.of("baz");

    private FindSeries s1;
    private FindSeries s2;
    private FindSeries s3;

    @Before
    public void setup() {
        s1 = new FindSeries(errors, ImmutableSet.of(a), false);
        s2 = new FindSeries(ImmutableList.of(), ImmutableSet.of(a, b), false);
        s3 = new FindSeries(ImmutableList.of(), ImmutableSet.of(c), false);
    }

    @Test
    public void reduceTest() throws Exception {
        final Set<Series> all = ImmutableSet.of(a, b, c);

        assertEquals(new FindSeries(errors, all, false),
            FindSeries.reduce(OptionalLimit.empty()).collect(ImmutableList.of(s1, s2, s3)));

        final FindSeries find =
            FindSeries.reduce(OptionalLimit.of(2)).collect(ImmutableList.of(s1, s2, s3));

        assertEquals(2, find.getSeries().size());
        assertTrue(find.isLimited());
        assertEquals(1, Sets.difference(all, find.getSeries()).size());
    }
}
