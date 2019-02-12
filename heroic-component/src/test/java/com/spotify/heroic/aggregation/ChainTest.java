package com.spotify.heroic.aggregation;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@RunWith(MockitoJUnitRunner.class)
public class ChainTest {
    @Mock
    private AggregationInstance a;

    @Mock
    private AggregationInstance c;

    @Mock
    private AggregationInstance cdis;

    @Mock
    private AggregationInstance cred;

    @Before
    public void setup() {
        doReturn(false).when(a).distributable();
        doThrow(new RuntimeException("not supported")).when(a).distributed();
        doThrow(new RuntimeException("not supported")).when(a).reducer();

        doReturn(true).when(c).distributable();
        doReturn(cdis).when(c).distributed();
        doReturn(cred).when(c).reducer();
    }

    @Test
    public void testDistributed() throws IOException {
        assertDistributed(ChainInstance.of(c, cdis), cred, ChainInstance.of(c, c));
        assertDistributed(ChainInstance.of(c, cdis), ChainInstance.of(cred, a),
            ChainInstance.of(c, c, a));
        assertDistributed(EmptyInstance.INSTANCE, ChainInstance.of(EmptyInstance.INSTANCE, a, a),
            ChainInstance.of(a, a));
    }

    @Test
    public void testNestedDistributed() throws IOException {
        assertDistributed(ChainInstance.of(c, ChainInstance.of(c, cdis)), cred,
            ChainInstance.of(c, ChainInstance.of(c, c)));

        assertDistributed(ChainInstance.of(c, c, cdis), ChainInstance.of(cred, a, c, c, c),
            ChainInstance.of(c, ChainInstance.of(c, c, a, c), ChainInstance.of(c, c)));
    }

    private void assertDistributed(
        final AggregationInstance distributed, final AggregationInstance reducer,
        final AggregationInstance input
    ) {
        assertEquals(distributed, input.distributed());
        assertEquals(reducer, input.reducer());
    }
}
