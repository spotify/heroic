package com.spotify.heroic.lifecycle;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class LifeCycleTest {
    @Mock
    LifeCycle a;

    @Mock
    LifeCycle b;

    @Mock
    LifeCycle c;

    @Test
    public void emptyTest() {
        LifeCycle.empty().install();
    }

    @Test
    public void manyLifeCycleTest() {
        final LifeCycle combined = LifeCycle.combined(ImmutableList.of(a, b));
        final LifeCycle all = LifeCycle.combined(ImmutableList.of(combined, c, LifeCycle.empty()));

        assertTrue(all instanceof ManyLifeCycle);
        final ManyLifeCycle many = (ManyLifeCycle) all;

        assertEquals(ImmutableList.of(a, b, c), many.getLifeCycles());
        assertEquals("+[a, b, c]", many.toString());

        many.install();

        verify(a).install();
        verify(b).install();
        verify(c).install();

        assertEquals(LifeCycle.combined(
            Stream.<LifeCycle>builder().add(combined).add(c).add(LifeCycle.empty()).build()), all);
    }
}
