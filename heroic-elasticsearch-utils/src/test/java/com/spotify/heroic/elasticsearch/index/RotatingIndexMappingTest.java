package com.spotify.heroic.elasticsearch.index;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Before;
import org.junit.Test;

import com.spotify.heroic.model.DateRange;

public class RotatingIndexMappingTest {
    private RotatingIndexMapping rotating;

    private long interval = 1000;
    private String pattern = "index-%s";
    private long retention = 4000;

    @Before
    public void setup() {
        rotating = new RotatingIndexMapping(interval, pattern, retention);
    }

    @Test
    public void testBasicRange() throws NoIndexSelectedException {
        final DateRange range = new DateRange(0, 4000);
        final String[] indices = rotating.indices(range, 8000);
        assertArrayEquals(new String[] { "index-4000" }, indices);
    }

    @Test(expected = NoIndexSelectedException.class)
    public void testNoIndexSelectedException() throws NoIndexSelectedException {
        final DateRange range = new DateRange(0, 4000);
        rotating.indices(range, 9000);
    }
}