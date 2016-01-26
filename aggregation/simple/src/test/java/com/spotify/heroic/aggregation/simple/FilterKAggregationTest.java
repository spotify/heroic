package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.AggregationState;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilterKAggregationTest {

    @Test
    public void testFilterKSession() {
        final GroupingAggregation g1 = new GroupInstance(
            Optional.of(ImmutableList.of("site", "host")), EmptyInstance.INSTANCE);
        final GroupingAggregation g2 =
            new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);
        final ChainInstance chain = new ChainInstance(
            Optional.of(ImmutableList.of(g1, g2)));

        final TopKInstance t1 = new TopKInstance(2, chain);
        final BottomKInstance b1 = new BottomKInstance(1, t1);

        final List<AggregationState> states = new ArrayList<>();

        final Series s1 = Series.of("foo", ImmutableMap.of("site", "sto", "host", "a"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "sto", "host", "b"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "lon", "host", "b"));
        final Series s4 = Series.of("foo", ImmutableMap.of("host", "c"));

        states.add(AggregationState.forSeries(s1));
        states.add(AggregationState.forSeries(s2));
        states.add(AggregationState.forSeries(s3));
        states.add(AggregationState.forSeries(s4));

        final AggregationSession session =
            b1.session(states, new DateRange(0, 10000)).getSession();

        session.updatePoints(s4.getTags(), ImmutableSet.of(s4),
            ImmutableList.of(new Point(1, 1.0)));
        session.updatePoints(s3.getTags(), ImmutableSet.of(s3),
            ImmutableList.of(new Point(2, 2.0)));
        session.updatePoints(s2.getTags(), ImmutableSet.of(s2),
            ImmutableList.of(new Point(3, 3.0)));
        session.updatePoints(s1.getTags(), ImmutableSet.of(s1),
            ImmutableList.of(new Point(4, 4.0)));

        /* Before the time series reach the bottomk/topk aggregation, the aggregated areas should be
           {empty: 1, lon: 2, sto: 7}. And we apply topk(2) | bottomk(1) so we expect lon to be
            the only group at the end of the aggregation
         */

        final List<AggregationData> result = session.result().getResult();

        assertEquals(1, result.size());

        AggregationData first = result.get(0);

        if (first.getGroup().equals(ImmutableMap.of("site", "lon"))) {
            assertEquals(ImmutableList.of(new Point(2, 2.0)), first.getMetrics().getData());
        } else {
            Assert.fail("unexpected group: " + first.getGroup());
        }
    }
}
