package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.Point;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class NotNegativeTest {

    final private NotNegativeInstance notNegativeInstance = NotNegativeInstance.INSTANCE;
    final private AggregationSession session = notNegativeInstance.session(new DateRange(0, 10000));

    @Test
    public void testLeaveOnlyPositive() throws Exception {
        // Test positive deltas
        ArrayList<Point> points = new ArrayList<Point>();
        points.add(new Point(TimeUnit.SECONDS.toMillis(1), 2));
        points.add(new Point(TimeUnit.SECONDS.toMillis(2), 3));
        points.add(new Point(TimeUnit.SECONDS.toMillis(3), -1));
        points.add(new Point(TimeUnit.SECONDS.toMillis(4), 5));
        points.add(new Point(TimeUnit.SECONDS.toMillis(5), -6));

        ArrayList<Point> expected = new ArrayList<Point>();
        expected.add(new Point(TimeUnit.SECONDS.toMillis(1), 2));
        expected.add(new Point(TimeUnit.SECONDS.toMillis(2), 3));
        expected.add(new Point(TimeUnit.SECONDS.toMillis(4), 5));

        assertEquals(expected, notNegativeInstance.filterPoints(points));
    }
}
