package com.spotify.heroic.aggregation.simple;

import static org.junit.Assert.assertEquals;

import com.spotify.heroic.metric.Point;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class RateTest {

    final private RateInstance rateInstance = new RateInstance();

    @Test
    public void testRateNoReset() throws Exception {
        List<Point> points = new ArrayList<>();
        points.add(new Point(1540000000000L, 10));
        points.add(new Point(1540000030000L, 20));
        points.add(new Point(1540000060000L, 100));

        ArrayList<Point> expected = new ArrayList<>();
        expected.add(new Point(1540000030000L, 10));
        expected.add(new Point(1540000060000L, 80));

        assertEquals(expected, rateInstance.computeDiff(points));
    }

    @Test
    public void testRateCounterReset() throws Exception {
        List<Point> points = new ArrayList<>();
        points.add(new Point(1540000000000L, 10));
        points.add(new Point(1540000030000L, 0));
        points.add(new Point(1540000060000L, 100));

        ArrayList<Point> expected = new ArrayList<>();
        expected.add(new Point(1540000030000L, 0));
        expected.add(new Point(1540000060000L, 100));

        assertEquals(expected, rateInstance.computeDiff(points));
    }

}
