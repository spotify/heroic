package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.Point;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DeltaPerSecondTest {

    final private DeltaPerSecondInstance deltaInstance = new DeltaPerSecondInstance();
    final private AggregationSession session = deltaInstance.session(new DateRange(0, 10000));

    @Test
    public void testComputePositiveDeltaPerSeconds() throws Exception {
        // Test positive deltas
        ArrayList<Point> points = new ArrayList<Point>();
        points.add(new Point(TimeUnit.SECONDS.toMillis(1), 2));
        points.add(new Point(TimeUnit.SECONDS.toMillis(2), 3));
        points.add(new Point(TimeUnit.SECONDS.toMillis(3), 100));

        ArrayList<Point> expected = new ArrayList<Point>();
        expected.add(new Point(TimeUnit.SECONDS.toMillis(2), 1));
        expected.add(new Point(TimeUnit.SECONDS.toMillis(3), 97));

        assertEquals(expected, deltaInstance.computeDiff(points));
    }

        @Test
    public void testComputeForSpreadPoints() throws Exception {
        // Test positive deltas
        ArrayList<Point> points = new ArrayList<Point>();
        points.add(new Point(TimeUnit.SECONDS.toMillis(1), 2));
        points.add(new Point(TimeUnit.SECONDS.toMillis(2), 3));
        points.add(new Point(TimeUnit.SECONDS.toMillis(30), 100));

        ArrayList<Point> expected = new ArrayList<Point>();
        expected.add(new Point(TimeUnit.SECONDS.toMillis(2), 1));
        expected.add(new Point(TimeUnit.SECONDS.toMillis(30), 97.0/28.0));

        assertEquals(expected, deltaInstance.computeDiff(points));
    }

    @Test
    public void testComputeNegativeDeltaPerSecond() throws Exception {
        // Test negative deltas
        ArrayList<Point> points = new ArrayList<Point>();
        points.add(new Point(TimeUnit.SECONDS.toMillis(1), 5));
        points.add(new Point(TimeUnit.SECONDS.toMillis(2),25));
        points.add(new Point(TimeUnit.SECONDS.toMillis(3), 0));

        ArrayList<Point> expected = new ArrayList<Point>();
        expected.add(new Point(TimeUnit.SECONDS.toMillis(2), 20));
        expected.add(new Point(TimeUnit.SECONDS.toMillis(3), -25));

        assertEquals(expected, deltaInstance.computeDiff(points));
    }

}
