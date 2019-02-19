package com.spotify.heroic.metadata;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.AbstractReducedResultTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DeleteSeriesTest extends AbstractReducedResultTest {
    private DeleteSeries s1;
    private DeleteSeries s2;
    private DeleteSeries s3;

    @Before
    public void setup() {
        s1 = new DeleteSeries(errors, 3, 0);
        s2 = new DeleteSeries(ImmutableList.of(), 0, 4);
    }

    @Test
    public void reduceTest() throws Exception {
        assertEquals(new DeleteSeries(errors, 3, 4),
            DeleteSeries.reduce().collect(ImmutableList.of(s1, s2)));
    }
}
