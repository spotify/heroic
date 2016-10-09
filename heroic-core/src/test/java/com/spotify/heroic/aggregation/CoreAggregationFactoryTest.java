package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class CoreAggregationFactoryTest {
    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final RuntimeException e = new RuntimeException("exception");

    private Aggregation aggregation;
    private CoreAggregationFactory factory;

    @Before
    public void setup() {
        aggregation = mock(Aggregation.class);

        final AggregationDSL throwing = mock(AggregationDSL.class);
        doThrow(e).when(throwing).build(any(AggregationArguments.class));

        final AggregationDSL working = mock(AggregationDSL.class);
        doReturn(aggregation).when(working).build(any(AggregationArguments.class));

        final Map<String, AggregationDSL> builderMap =
            ImmutableMap.of("throwing", throwing, "working", working);
        factory = new CoreAggregationFactory(builderMap);
    }

    @Test
    public void builderMissing() {
        expected.expect(MissingAggregation.class);
        expected.expectMessage("Missing aggregation missing");

        factory.build("missing", ImmutableList.of(), ImmutableMap.of());
    }

    @Test
    public void builderThrowing() {
        expected.expect(RuntimeException.class);
        expected.expectMessage("throwing: exception");

        factory.build("throwing", ImmutableList.of(), ImmutableMap.of());
    }

    @Test
    public void builderWorking() {
        assertEquals(aggregation, factory.build("working", ImmutableList.of(), ImmutableMap.of()));
    }
}
