package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
                protected AggregationInstance newInstance(
                    final Optional<List<String>> of, final AggregationInstance each
                ) {
                    return this;
                }
            });

        doReturn(key1).when(a).key(key1);
        doReturn(key2).when(a).key(key2);

        a.map(ImmutableList.of(state1, state2));
    }

    @Test
    public void testChainedSessions() {
        final GroupingAggregation g1 =
            new GroupInstance(Optional.of(ImmutableList.of("site", "host")),
                EmptyInstance.INSTANCE);
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

        session.updatePoints(s4.getTags(), ImmutableList.of(new Point(4, 4.0)));
        session.updatePoints(s3.getTags(), ImmutableList.of(new Point(3, 3.0)));
        session.updatePoints(s2.getTags(), ImmutableList.of(new Point(2, 2.0)));
        session.updatePoints(s1.getTags(), ImmutableList.of(new Point(1, 1.0)));

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

    /**
     * Checks that the distribute aggregation for Grouping aggregations are composed out of the
     * distributed aggregation for the child clause.
     */
    @Test
    public void testDistributedAggregation() {
        final AggregationInstance distributed = mock(AggregationInstance.class);
        final AggregationInstance next = mock(AggregationInstance.class);

        final AggregationInstance each = mock(AggregationInstance.class);
        final Optional<List<String>> of = Optional.empty();
        final SimpleGroup g = spy(new SimpleGroup(of, each));

        doReturn(distributed).when(each).distributed();
        doReturn(next).when(g).newInstance(of, distributed);

        assertEquals(next, g.distributed());

        verify(each).distributed();
        verify(g).newInstance(of, distributed);
    }

    @Test
    public void testCombiner() {
        final AggregationInstance each = mock(AggregationInstance.class);
        final Optional<List<String>> of = Optional.empty();
        final SimpleGroup g = spy(new SimpleGroup(of, each));

        final ReducerSession r = mock(ReducerSession.class);
        final DateRange range = mock(DateRange.class);

        final ReducerResult result = new ReducerResult(ImmutableList.of(), Statistics.empty());

        doReturn(r).when(each).reducer(range);
        doReturn(result).when(r).result();

        final AggregationCombiner combiner = g.combiner(range);

        final List<List<ShardedResultGroup>> all = new ArrayList<>();

        final ImmutableMap<String, String> shard = ImmutableMap.of();
        final MetricCollection empty = MetricCollection.points(ImmutableList.of());

        final Map<String, String> k1 = ImmutableMap.of("id", "a");
        final SeriesValues g1 = SeriesValues.of("id", "a");
        final Map<String, String> k2 = ImmutableMap.of("id", "b");
        final SeriesValues g2 = SeriesValues.of("id", "b");

        final ShardedResultGroup a = new ShardedResultGroup(shard, k1, g1, empty, 0);
        final ShardedResultGroup b = new ShardedResultGroup(shard, k1, g1, empty, 0);
        final ShardedResultGroup c = new ShardedResultGroup(shard, k2, g2, empty, 0);

        all.add(ImmutableList.of(a, b, c));

        assertTrue(combiner.combine(all).isEmpty());

        verify(r, times(2)).updatePoints(ImmutableMap.of("id", "a"), ImmutableList.of());
        verify(r, times(1)).updatePoints(ImmutableMap.of("id", "b"), ImmutableList.of());
    }

    public static class SimpleGroup extends GroupingAggregation {
        public SimpleGroup(final Optional<List<String>> of, final AggregationInstance each) {
            super(of, each);
        }

        protected Map<String, String> key(Map<String, String> input) {
            return input;
        }

        @Override
        protected AggregationInstance newInstance(
            Optional<List<String>> of, AggregationInstance each
        ) {
            return new SimpleGroup(of, each);
        }
    }
}
