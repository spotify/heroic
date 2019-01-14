package com.spotify.heroic.aggregation;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Point;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GroupingAggregationTest {
    @Mock
    DateRange range;

    @Mock
    Map<String, String> key1;

    @Mock
    Map<String, String> key2;

    @Test
    public void testChainedSessions() {
        final GroupingAggregation g1 =
            new GroupInstance(Optional.of(ImmutableList.of("site", "host")),
                EmptyInstance.INSTANCE);
        final GroupingAggregation g2 =
            new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);

        final ChainInstance chain = new ChainInstance(ImmutableList.of(g1, g2));

        final Set<Series> series = new HashSet<>();

        final Series s1 = Series.of("foo", ImmutableMap.of("site", "sto", "host", "a"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "sto", "host", "b"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "lon", "host", "b"));
        final Series s4 = Series.of("foo", ImmutableMap.of("host", "c"));

        series.add(s1);
        series.add(s2);
        series.add(s3);
        series.add(s4);

        final AggregationSession session = chain.session(new DateRange(0, 10000));

        session.updatePoints(s4.getTags(), ImmutableSet.of(s4),
            ImmutableList.of(new Point(4, 4.0)));
        session.updatePoints(s3.getTags(), ImmutableSet.of(s3),
            ImmutableList.of(new Point(3, 3.0)));
        session.updatePoints(s2.getTags(), ImmutableSet.of(s2),
            ImmutableList.of(new Point(2, 2.0)));
        session.updatePoints(s1.getTags(), ImmutableSet.of(s1),
            ImmutableList.of(new Point(1, 1.0)));

        final List<AggregationOutput> result = session.result().getResult();

        assertEquals(3, result.size());

        final Set<Map<String, String>> expected =
            result.stream().map(AggregationOutput::getKey).collect(Collectors.toSet());

        for (final AggregationOutput data : result) {
            if (data.getKey().equals(ImmutableMap.of("site", "lon"))) {
                assertEquals(ImmutableList.of(new Point(3, 3.0)), data.getMetrics().getData());
                expected.remove(ImmutableMap.of("site", "lon"));
                continue;
            }

            if (data.getKey().equals(ImmutableMap.of("site", "sto"))) {
                assertEquals(ImmutableList.of(new Point(1, 1.0), new Point(2, 2.0)),
                    data.getMetrics().getData());
                expected.remove(ImmutableMap.of("site", "sto"));
                continue;
            }

            if (data.getKey().equals(ImmutableMap.of())) {
                assertEquals(ImmutableList.of(new Point(4, 4.0)), data.getMetrics().getData());
                expected.remove(ImmutableMap.of());
                continue;
            }

            Assert.fail("unexpected group: " + data.getKey());
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
