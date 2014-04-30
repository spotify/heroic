package com.spotify.heroic.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.TimeSerieSlice;

public class TimeSerieSliceTest {
    private static final String KEY = "key";
    private static final Map<String, String> TAGS = new HashMap<String, String>();
    final TimeSerie TS = new TimeSerie("key_a", TAGS);
    final TimeSerie TS_B = new TimeSerie("key_b", TAGS);

    @Test
    public void testEquality() throws Exception {
        final TimeSerie timeSerieA = Mockito.mock(TimeSerie.class);
        final TimeSerieSlice a = new TimeSerieSlice(timeSerieA, new DateRange(0, 42));
        final TimeSerieSlice b = new TimeSerieSlice(timeSerieA, new DateRange(0, 42));
        final TimeSerieSlice c = new TimeSerieSlice(timeSerieA, new DateRange(0, 50));
        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testSlice() throws Exception {
        final TimeSerieSlice sliceA = TS.slice(0, 1000);
        Assert.assertEquals(0, sliceA.getRange().start());
        Assert.assertEquals(1000, sliceA.getRange().end());
    }

    @Test
    public void testSliceB() throws Exception {
        final TimeSerieSlice sliceA = TS.slice(0, 1000);
        final TimeSerieSlice sliceB = TS.slice(1000, 2000);
        final TimeSerieSlice sliceC = sliceA.join(sliceB);

        Assert.assertEquals(sliceA.getRange().start(), sliceC.getRange().start());
        Assert.assertEquals(sliceB.getRange().end(), sliceC.getRange().end());
    }

    @Test
    public void testJoinAll() throws Exception {
        List<TimeSerieSlice> slices = new ArrayList<TimeSerieSlice>();

        slices.add(TS.slice(1000, 2000));
        slices.add(TS.slice(3000, 4000));
        slices.add(TS.slice(0, 1000));

        final List<TimeSerieSlice> expected = new ArrayList<TimeSerieSlice>();

        expected.add(TS.slice(0, 2000));
        expected.add(TS.slice(3000, 4000));

        final List<TimeSerieSlice> result = TimeSerieSlice.joinAll(slices);
        Assert.assertEquals(expected, result);
    }

    /**
     * Different time series should not be joinable.
     * 
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testJoinAllDifferentTimeSeries() throws Exception {
        List<TimeSerieSlice> slices = new ArrayList<TimeSerieSlice>();

        slices.add(TS.slice(1000, 2000));
        slices.add(TS.slice(3000, 4000));
        slices.add(TS_B.slice(0, 1000));

        TimeSerieSlice.joinAll(slices);
    }
}
