package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
public class FilterKThresholdAggregationTest {

    @Test
    public void testFilterKAreaSession() {
        final GroupingAggregation g1 =
            new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);

        final AggregationInstance a1 = ChainInstance.of(g1, new AboveKInstance(1));
        final AggregationInstance b1 = ChainInstance.of(a1, new BelowKInstance(2.5));

        final Set<Series> states = new HashSet<>();

        final Series s1 = Series.of("foo", ImmutableMap.of("site", "sto", "host", "a"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "ash", "host", "b"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "lon", "host", "c"));

        states.add(s1);
        states.add(s2);
        states.add(s3);

        final AggregationSession session = b1.session(new DateRange(0, 10000));

        session.updatePoints(s1.getTags(), ImmutableSet.of(s1),
            ImmutableList.of(new Point(2, 2.0), new Point(3, 2.0)));
        session.updatePoints(s2.getTags(), ImmutableSet.of(s2),
            ImmutableList.of(new Point(2, 3.0), new Point(3, 3.0)));
        session.updatePoints(s3.getTags(), ImmutableSet.of(s3),
            ImmutableList.of(new Point(2, 1.0), new Point(3, 1.0)));

        final List<AggregationOutput> result = session.result().getResult();

        assertEquals(1, result.size());

        AggregationOutput first = result.get(0);

        if (first.getKey().equals(ImmutableMap.of("site", "sto"))) {
            assertEquals(ImmutableList.of(new Point(2, 2.0), new Point(3, 2.0)),
                first.getMetrics().getData());
        } else {
            Assert.fail("unexpected group: " + first.getKey());
        }
    }
}
