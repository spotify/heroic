package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChainInstanceTest {
    @Test
    public void pickLastValidCadence() {
        final AggregationInstance a = mock(AggregationInstance.class);
        final AggregationInstance b = mock(AggregationInstance.class);
        final AggregationInstance c = mock(AggregationInstance.class);
        when(a.cadence()).thenReturn(-1L);
        when(b.cadence()).thenReturn(2L);
        when(c.cadence()).thenReturn(-1L);

        final AggregationInstance chain = ChainInstance.fromList(ImmutableList.of(a, b, c));
        assertEquals(2, chain.cadence());
    }
}
