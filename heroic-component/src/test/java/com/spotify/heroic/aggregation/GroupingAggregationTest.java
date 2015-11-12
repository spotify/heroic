package com.spotify.heroic.aggregation;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Point;

@RunWith(MockitoJUnitRunner.class)
public class GroupingAggregationTest {
    @Mock
    DateRange range;

    @Mock
    Map<String, String> key1;

    @Mock
    Map<String, String> key2;

    @Mock
    AggregationState state1;

    @Mock
    AggregationState state2;

    @Before
    public void setup() {
        doReturn(key1).when(state1).getKey();
        doReturn(key2).when(state2).getKey();
    }

    @Test
    public void testSession() {
        final AggregationInstance each = mock(AggregationInstance.class);

        final GroupingAggregation a =
                spy(new GroupingAggregation(Optional.of(ImmutableList.of("group")), each) {
                    @Override
                    protected Map<String, String> key(Map<String, String> input) {
                        return null;
                    }

                    @Override
                    protected AggregationInstance newInstance(final Optional<List<String>> of,
                            final AggregationInstance each) {
                        return this;
                    }
                });

        doReturn(key1).when(a).key(key1);
        doReturn(key2).when(a).key(key2);

        a.map(ImmutableList.of(state1, state2));
    }

    @Test
    public void testChainedSessions() {
        final GroupingAggregation g1 = new GroupInstance(
                Optional.of(ImmutableList.of("site", "host")), EmptyInstance.INSTANCE);
        final GroupingAggregation g2 =
                new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);

        final ChainInstance chain = new ChainInstance(ImmutableList.of(g1, g2));

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
                chain.session(states, new DateRange(0, 10000)).getSession();

        session.updatePoints(s4.getTags(), ImmutableSet.of(s4),
                ImmutableList.of(new Point(4, 4.0)));
        session.updatePoints(s3.getTags(), ImmutableSet.of(s3),
                ImmutableList.of(new Point(3, 3.0)));
        session.updatePoints(s2.getTags(), ImmutableSet.of(s2),
                ImmutableList.of(new Point(2, 2.0)));
        session.updatePoints(s1.getTags(), ImmutableSet.of(s1),
                ImmutableList.of(new Point(1, 1.0)));

        final List<AggregationData> result = session.result().getResult();

        assertEquals(3, result.size());

        final Set<Map<String, String>> expected =
                result.stream().map(AggregationData::getGroup).collect(Collectors.toSet());

        for (final AggregationData data : result) {
            if (data.getGroup().equals(ImmutableMap.of("site", "lon"))) {
                assertEquals(ImmutableList.of(new Point(3, 3.0)), data.getMetrics().getData());
                expected.remove(ImmutableMap.of("site", "lon"));
                continue;
            }

            if (data.getGroup().equals(ImmutableMap.of("site", "sto"))) {
                assertEquals(ImmutableList.of(new Point(1, 1.0), new Point(2, 2.0)),
                        data.getMetrics().getData());
                expected.remove(ImmutableMap.of("site", "sto"));
                continue;
            }

            if (data.getGroup().equals(ImmutableMap.of())) {
                assertEquals(ImmutableList.of(new Point(4, 4.0)), data.getMetrics().getData());
                expected.remove(ImmutableMap.of());
                continue;
            }

            Assert.fail("unexpected group: " + data.getGroup());
        }
    }
}
