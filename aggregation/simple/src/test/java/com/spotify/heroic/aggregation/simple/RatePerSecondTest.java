package com.spotify.heroic.aggregation.simple;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Point;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class RatePerSecondTest {
    final private long startRange = 1571686200000L;
    final private long endRange = 1571687100000L;
    final private DateRange fifteenMinuteRange = new DateRange(startRange, endRange);
    final private Map<String, String> group = ImmutableMap.of();
    final private Set<Series> series = ImmutableSet.of();

    private List<Point> makePoints() {
        final ArrayList<Point> points = new ArrayList<>();
        points.add(new Point(1571686240329L,100.0));
        points.add(new Point(1571686300329L,200.0));
        points.add(new Point(1571686360329L,500.0));
        points.add(new Point(1571686420329L,1_000.0));
        points.add(new Point(1571686480329L,1_000.0));
        points.add(new Point(1571686540329L,1_100.0));
        points.add(new Point(1571686600329L,1_200.0));
        points.add(new Point(1571686660329L,1_300.0));
        points.add(new Point(1571686720329L,1_500.0));
        points.add(new Point(1571686780329L,1_600.0));
        points.add(new Point(1571686840329L,1_900.0));
        points.add(new Point(1571686900329L,3_100.0));
        points.add(new Point(1571686960329L,3_200.0));
        points.add(new Point(1571687020329L,3_300.0));
        points.add(new Point(1571687080329L,3_400.0));
        return points;
    }

    @Test
    public void testTwoMinuteSamplingWindowNoCounterResets() {
        final long sampleResolution = Duration.ofMinutes(2).toMillis();
        final RatePerSecondInstance rateInstance =
            new RatePerSecondInstance(sampleResolution, sampleResolution);
        final AggregationSession session = rateInstance.session(fifteenMinuteRange);

        session.updatePoints(group, series,  makePoints());

        final List<Point> p = (List<Point>)
            session.result().getResult().get(0).getMetrics().data();

        assertEquals(1.66, p.get(0).getValue(), 0.01);
        assertEquals(8.33, p.get(1).getValue(), 0.01);
        assertEquals(1.66, p.get(6).getValue(), 0.01);
    }

    @Test
    public void testSamplingWindowEqualResolutionNoCounterResets() {
        final long sampleResolution = Duration.ofMinutes(15).toMillis();
        final RatePerSecondInstance rateInstance =
            new RatePerSecondInstance(sampleResolution, sampleResolution);
        final AggregationSession session = rateInstance.session(fifteenMinuteRange);

        session.updatePoints(group, series,  makePoints());

        final List<Point> p = (List<Point>)
            session.result().getResult().get(0).getMetrics().data();

        assertEquals(3.928, p.get(0).getValue(), 0.01);
    }

    @Test
    public void testSamplingWindowEqualResolutionCounterReset() {
        final long sampleResolution = Duration.ofMinutes(15).toMillis();
        final RatePerSecondInstance rateInstance =
            new RatePerSecondInstance(sampleResolution, sampleResolution);
        final AggregationSession session = rateInstance.session(fifteenMinuteRange);

        final List<Point> points = makePoints();
        points.add(1, new Point(1571686300329L, 0D));
        points.remove(2);

        session.updatePoints(group, series,  points);

        final List<Point> p = (List<Point>)
            session.result().getResult().get(0).getMetrics().data();

        assertEquals(4.047, p.get(0).getValue(), 0.01);
    }



}
