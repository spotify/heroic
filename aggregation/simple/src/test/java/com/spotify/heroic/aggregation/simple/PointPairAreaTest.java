package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.Point;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PointPairAreaTest {
    private static final double DELTA = 1e-10;

    @Test
    public void testComputePositivePoints() throws Exception {
        // Create two points that form a rectangle with the X-axis
        final Point a = new Point(2, 4);
        final Point b = new Point(4, 4);

        // Expected rectangle area 2 * 4
        assertEquals(8.0, PointPairArea.computeArea(a, b), DELTA);
    }

    @Test
    public void testComputePositivePointsTriangle() throws Exception {
        // Create two points that form a triangle with the X-axis
        final Point a = new Point(3, 2);
        final Point b = new Point(5, 0);

        // Expected triangle area (2 * 2 / 2)
        assertEquals(2.0, PointPairArea.computeArea(a, b), DELTA);
    }

    @Test
    public void testComputePositivePointsRectangleAndTriangle() throws Exception {
        // Create two points that form a rectangle with the X-axis and a triangle on top of it
        final Point a = new Point(2, 2);
        final Point b = new Point(7, 4);

        // Expected rectangle area (5 * 2) + triangle area (5  * 2 / 2)
        assertEquals(15.0, PointPairArea.computeArea(a, b), DELTA);
    }

    @Test
    public void testComputeNegativePoints() throws Exception {
        // Create two points that form a rectangle
        final Point a = new Point(2, -4);
        final Point b = new Point(4, -4);

        // Expected rectangle area -(2 * 4)
        assertEquals(-8.0, PointPairArea.computeArea(a, b), DELTA);
    }

    @Test
    public void testComputeNegativePointsTriangle() throws Exception {
        // Create two points that form a triangle with the X-axis
        final Point a = new Point(3, -2);
        final Point b = new Point(5, 0);

        // Expected rectangle area -(2 * 2 / 2)
        assertEquals(-2.0, PointPairArea.computeArea(a, b), DELTA);
    }

    @Test
    public void testComputeNegativePointsRectangleAndTriangle() throws Exception {
        // Create two points that form a rectangle and a triangle on top of it
        final Point a = new Point(2, -2);
        final Point b = new Point(7, -4);

        // Expected rectangle area (5 * 2) + triangle area (5  * 2 / 2)
        assertEquals(-15.0, PointPairArea.computeArea(a, b), DELTA);
    }

    @Test
    public void testComputePositiveAndNegativePoints() throws Exception {
        // Create two points that form a rectangle above the X-axis and and another rectangle below
        final Point a = new Point(3, 2);
        final Point b = new Point(6, -1);

        // Expected  rectangle above (2 * 2 / 2) - rectangle below (1 * 1 / 2)
        assertEquals(1.5, PointPairArea.computeArea(a, b), DELTA);
    }

    @Test
    public void testComputeNegativeAndPositivePoints() throws Exception {
        // Create two points that form a rectangle below X-axis and and another rectangle above
        final Point a = new Point(3, -2);
        final Point b = new Point(6, 1);

        // Expected  rectangle below -(2 * 2 / 2) + rectangle above (1 * 1 / 2)
        assertEquals(-1.5, PointPairArea.computeArea(a, b), DELTA);
    }

    @Test
    public void testComputeZeroPoints() throws Exception {
        final Point a = new Point(3, 0);
        final Point b = new Point(6, 0);

        assertEquals(0, PointPairArea.computeArea(a, b), DELTA);
    }
}