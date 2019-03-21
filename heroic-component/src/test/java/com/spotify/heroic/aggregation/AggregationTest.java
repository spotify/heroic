package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;

/**
 * Tests for a bunch of aggregation archetypes.
 *
 * @author udoprog
 */
@RunWith(MockitoJUnitRunner.class)
public class AggregationTest {
    @Mock
    private Aggregation a;

    @Mock
    private AggregationInstance instance;

    private AggregationContext context;

    @Before
    public void setup() {
        doReturn(instance).when(a).apply(any(AggregationContext.class));
        context = AggregationContext.defaultInstance(Duration.of(1, TimeUnit.MINUTES));
    }

    @Test
    public void testEmptyInChain() {
        Aggregations.chain();
    }

    /**
     * Test that required tags works when elided for a given collection of aggregations.
     */
    @Test
    public void testTagElision() {
        final Aggregation g1 = Group.createFromAggregation(Optional.of(ImmutableList.of("foo")), Optional.of(a));

        final Aggregation g2 =
            Group.createFromAggregation(Optional.of(ImmutableList.of("bar")), Optional.of(new Chain(ImmutableList.of(
                // inner groups should _not_ force tags elision
                Group.createFromAggregation(Optional.of(ImmutableList.of("baz")), Optional.of(a))))));

        final AggregationInstance instance =
            Aggregations.chain(Optional.of(ImmutableList.of(g1, g2))).apply(context);

        final ChainInstance chain = (ChainInstance) instance;
        final GroupInstance g1i = (GroupInstance) chain.getChain().get(0);
        final GroupInstance g2i = (GroupInstance) chain.getChain().get(1);
        final GroupInstance g2i1i =
            (GroupInstance) (((GroupInstance) chain.getChain().get(1)).getEach());

        assertEquals(Optional.of(ImmutableSet.of("foo", "bar")),
            g1i.getOf().map(ImmutableSet::copyOf));
        assertEquals(Optional.of(ImmutableSet.of("bar")), g2i.getOf().map(ImmutableSet::copyOf));
        assertEquals(Optional.of(ImmutableSet.of("baz")), g2i1i.getOf().map(ImmutableSet::copyOf));
    }
}
