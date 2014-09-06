package com.spotify.heroic.model.filter;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.OrFilter;

public class OrFilterTest {
    @Test
    public void testOptimize() throws Exception {
        final Filter reference = Mockito.mock(Filter.class);
        Mockito.when(reference.optimize()).thenReturn(reference);

        final Filter nested = Mockito.mock(Filter.class);
        Mockito.when(nested.optimize()).thenReturn(nested);

        final OrFilter or1 = new OrFilter(
                Arrays.asList(new Filter[] { nested }));
        final OrFilter or2 = new OrFilter(Arrays.asList(new Filter[] { or1 }));

        Assert.assertEquals(nested, or2.optimize());
    }
}
