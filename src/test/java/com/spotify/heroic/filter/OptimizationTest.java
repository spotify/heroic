package com.spotify.heroic.filter;

import static com.spotify.heroic.filter.Filters.and;
import static com.spotify.heroic.filter.Filters.not;
import static com.spotify.heroic.filter.Filters.or;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class OptimizationTest {
    public static Filter mockFilter() {
        final Filter f = Mockito.mock(Filter.class);
        Mockito.when(f.optimize()).thenReturn(f);
        return f;
    }

    private final Filter a = mockFilter();
    private final Filter b = mockFilter();

    @Test
    public void optimizeAndTest() {
        final Filter ref = and(a, b).optimize();
        Assert.assertTrue(ref instanceof ManyTermsFilter);
        Assert.assertEquals(2, ((ManyTermsFilter) ref).terms().size());
        Assert.assertEquals(ref, and(a, b, b).optimize());
        Assert.assertEquals(ref, and(b, a).optimize());
        Assert.assertEquals(ref, and(b, and(a, b)).optimize());
        Assert.assertEquals(Filters.FALSE, and(a, b, not(b)).optimize());
    }

    @Test
    public void optimizeOrTest() {
        final Filter ref = or(a, b).optimize();
        Assert.assertTrue(ref instanceof ManyTermsFilter);
        Assert.assertEquals(2, ((ManyTermsFilter) ref).terms().size());
        Assert.assertEquals(ref, or(a, b, b).optimize());
        Assert.assertEquals(ref, or(b, a).optimize());
        Assert.assertEquals(ref, or(b, or(a, b)).optimize());
        Assert.assertEquals(Filters.TRUE, or(a, b, not(b)).optimize());
    }

    @Test
    public void optimizeNotNot() {
        Assert.assertEquals(a, not(not(a)).optimize());
    }
}
