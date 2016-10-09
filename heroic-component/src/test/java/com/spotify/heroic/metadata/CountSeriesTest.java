package com.spotify.heroic.metadata;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.AbstractReducedResultTest;
import com.spotify.heroic.test.LombokDataTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CountSeriesTest extends AbstractReducedResultTest {
    private CountSeries s1;
    private CountSeries s2;
    private CountSeries s3;

    @Before
    public void setup() {
        s1 = new CountSeries(errors, 3L, false);
        s2 = new CountSeries(ImmutableList.of(), 3L, false);
        s3 = new CountSeries(errors, 4L, true);
    }

    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(CountSeries.class);
    }

    @Test
    public void reduceTest() throws Exception {
        assertEquals(new CountSeries(errors, 6L, false),
            CountSeries.reduce().collect(ImmutableList.of(s1, s2)));

        assertEquals(new CountSeries(errors, 7L, true),
            CountSeries.reduce().collect(ImmutableList.of(s2, s3)));
    }
}
