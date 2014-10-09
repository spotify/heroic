package com.spotify.heroic.filter;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;

public class AndFilterTest {
    @Test
    public void testOptimize() throws Exception {
        final Filter reference = Mockito.mock(Filter.class);
        Mockito.when(reference.optimize()).thenReturn(reference);

        final Filter nested = Mockito.mock(Filter.class);
        Mockito.when(nested.optimize()).thenReturn(nested);

        final AndFilter and1 = new AndFilter(
                Arrays.asList(new Filter[] { nested }));
        final AndFilter and2 = new AndFilter(
                Arrays.asList(new Filter[] { and1 }));

        Assert.assertEquals(nested, and2.optimize());
    }
}
