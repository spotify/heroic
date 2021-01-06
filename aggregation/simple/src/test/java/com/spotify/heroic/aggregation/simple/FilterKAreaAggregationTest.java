package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationOutput;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.ChainInstance;
import com.spotify.heroic.aggregation.EmptyInstance;
import com.spotify.heroic.aggregation.GroupInstance;
import com.spotify.heroic.aggregation.GroupingAggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Point;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilterKAreaAggregationTest {

    @Test
    public void testFilterKAreaSession() {
        final GroupingAggregation g =
            new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);

        final AggregationInstance t1 = ChainInstance.of(g, new TopKInstance(2));
        final AggregationInstance b1 = ChainInstance.of(t1, new BottomKInstance(1));

        final Set<Series> series = new HashSet<>();

        final Series s1 = Series.of("foo", ImmutableMap.of("site", "sto"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "ash"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "lon"));
        final Series s4 = Series.of("foo", ImmutableMap.of("site", "sjc"));

        series.add(s1);
        series.add(s2);
        series.add(s3);
        series.add(s4);

        final AggregationSession session = b1.session(new DateRange(0, 10000));

        session.updatePoints(s1.getTags(), series,
            ImmutableList.of(new Point(1, 1.0), new Point(2, 1.0)));
        session.updatePoints(s2.getTags(), series,
            ImmutableList.of(new Point(1, 2.0), new Point(2, 2.0)));
        session.updatePoints(s3.getTags(), series,
            ImmutableList.of(new Point(1, 3.0), new Point(2, 3.0)));
        session.updatePoints(s4.getTags(), series,
            ImmutableList.of(new Point(1, 4.0), new Point(2, 4.0)
                , new Point(2, 4.0)));

        final List<AggregationOutput> result = session.result().getResult();

        assertEquals(1, result.size());

        AggregationOutput first = result.get(0);

        if (first.getKey().equals(ImmutableMap.of("site", "lon"))) {
            assertEquals(ImmutableList.of(new Point(1, 3.0), new Point(2, 3.0)),
                first.getMetrics().data());
        } else {
            Assert.fail("unexpected group: " + first.getKey());
        }
    }
}
