package com.spotify.heroic.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.SeriesSlice;

public class SeriesSliceTest {
    private static final Map<String, String> TAGS = new HashMap<String, String>();
    final Series TS = new Series("key_a", TAGS);
    final Series TS_B = new Series("key_b", TAGS);

    @Test
    public void testEquality() throws Exception {
        final Series seriesA = Mockito.mock(Series.class);
        final SeriesSlice a = new SeriesSlice(seriesA, new DateRange(0, 42));
        final SeriesSlice b = new SeriesSlice(seriesA, new DateRange(0, 42));
        final SeriesSlice c = new SeriesSlice(seriesA, new DateRange(0, 50));
        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testSlice() throws Exception {
        final SeriesSlice sliceA = TS.slice(0, 1000);
        Assert.assertEquals(0, sliceA.getRange().start());
        Assert.assertEquals(1000, sliceA.getRange().end());
    }

    @Test
    public void testSliceB() throws Exception {
        final SeriesSlice sliceA = TS.slice(0, 1000);
        final SeriesSlice sliceB = TS.slice(1000, 2000);
        final SeriesSlice sliceC = sliceA.join(sliceB);

        Assert.assertEquals(sliceA.getRange().start(), sliceC.getRange().start());
        Assert.assertEquals(sliceB.getRange().end(), sliceC.getRange().end());
    }

    @Test
    public void testJoinAll() throws Exception {
        List<SeriesSlice> slices = new ArrayList<SeriesSlice>();

        slices.add(TS.slice(1000, 2000));
        slices.add(TS.slice(3000, 4000));
        slices.add(TS.slice(0, 1000));

        final List<SeriesSlice> expected = new ArrayList<SeriesSlice>();

        expected.add(TS.slice(0, 2000));
        expected.add(TS.slice(3000, 4000));

        final List<SeriesSlice> result = SeriesSlice.joinAll(slices);
        Assert.assertEquals(expected, result);
    }

    /**
     * Different time series should not be joinable.
     * 
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testJoinAllDifferentTimeSeries() throws Exception {
        List<SeriesSlice> slices = new ArrayList<SeriesSlice>();

        slices.add(TS.slice(1000, 2000));
        slices.add(TS.slice(3000, 4000));
        slices.add(TS_B.slice(0, 1000));

        SeriesSlice.joinAll(slices);
    }
}
