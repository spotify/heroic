package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.Point;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class DeltaTest {

    final private DeltaInstance deltaInstance = new DeltaInstance();
    final private AggregationSession session = deltaInstance.session(new DateRange(0, 10000));

    @Test
    public void testComputePositiveDeltas() throws Exception {
        // Test positive deltas
        ArrayList<Point> points = new ArrayList<Point>();
        points.add(new Point(1, 2));
        points.add(new Point(2, 3));
        points.add(new Point(3, 100));

        ArrayList<Point> expected = new ArrayList<Point>();
        expected.add(new Point(2, 1));
        expected.add(new Point(3, 97));

        assertEquals(expected, deltaInstance.computeDiff(points));
    }

    @Test
    public void testComputeNegativeDelta() throws Exception {
        // Test negative deltas
        ArrayList<Point> points = new ArrayList<Point>();
        points.add(new Point(1, 5));
        points.add(new Point(2, 25));
        points.add(new Point(3, 0));

        ArrayList<Point> expected = new ArrayList<Point>();
        expected.add(new Point(2, 20));
        expected.add(new Point(3, -25));

        assertEquals(expected, deltaInstance.computeDiff(points));
    }

}
