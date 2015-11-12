package com.spotify.heroic.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class FilterOptimizerTest {
    private static final FilterFactory f = new CoreFilterFactory();

    public static Filter mockFilter() {
        final Filter f = Mockito.mock(Filter.class);
        Mockito.when(f.optimize()).thenReturn(f);
        return f;
    }

    private final Filter a = f.hasTag("a");
    private final Filter b = f.hasTag("b");
    private final Filter c = f.hasTag("c");
    private final Filter tag1a = f.matchTag("foo", "a");
    private final Filter tag1b = f.matchTag("foo", "b");

    private final Filter s1a = f.startsWith("foo", "abc");
    private final Filter s1b = f.startsWith("foo", "abcdef");

    @Test
    public void optimizeAndTest() {
        final Filter ref = f.and(a, b).optimize();
        Assert.assertTrue(ref instanceof Filter.MultiArgs);
        Assert.assertEquals(2, ((Filter.MultiArgs<?>) ref).terms().size());
        Assert.assertEquals(ref, f.and(a, b, b).optimize());
        Assert.assertEquals(ref, f.and(b, a).optimize());
        Assert.assertEquals(ref, f.and(b, f.and(a, b)).optimize());
    }

    @Test
    public void optimizeOrTest() {
        final Filter ref = f.or(a, b).optimize();
        Assert.assertTrue(ref instanceof Filter.MultiArgs);
        Assert.assertEquals(2, ((Filter.MultiArgs<?>) ref).terms().size());
        Assert.assertEquals(ref, f.or(a, b, b).optimize());
        Assert.assertEquals(ref, f.or(b, a).optimize());
        Assert.assertEquals(ref, f.or(b, f.or(a, b)).optimize());
    }

    @Test
    public void testSortOrder() {
        final List<List<Filter>> inputs = ImmutableList.<List<Filter>> of(ImmutableList.of(a, b, c),
                ImmutableList.of(c, b, a), ImmutableList.of(c, b, a));

        final List<Filter> reference = ImmutableList.of(a, b, c);

        for (final List<Filter> input : inputs) {
            final List<Filter> sorted = new ArrayList<>(new TreeSet<>(input));
            Assert.assertEquals(reference, sorted);
        }
    }

    @Test
    public void testOrFlatten() {
        Assert.assertEquals(f.or(a, b, c), f.or(a, f.or(b, c)).optimize());
    }

    @Test
    public void testAndFlatten() {
        Assert.assertEquals(f.and(a, b, c), f.and(a, f.and(b, c)).optimize());
    }

    @Test
    public void optimizeNotNot() {
        Assert.assertEquals(a, f.not(f.not(a)).optimize());
    }

    /**
     * The same expression, but inverted cannot co-exist in the same and statement.
     */
    @Test
    public void testAndContradiction1() throws Exception {
        Assert.assertEquals(f.f(), f.and(a, f.not(a)).optimize());
    }

    /**
     * A match tag, with two different value conditions cannot co-exist in the same and statement.
     */
    @Test
    public void testAndContradiction2() throws Exception {
        Assert.assertEquals(f.f(), f.and(tag1a, tag1b).optimize());
    }

    /**
     * The same expression, but inverted cannot co-exist in the same and statement.
     */
    @Test
    public void testOrTautology1() throws Exception {
        Assert.assertEquals(f.t(), f.or(a, f.not(a)).optimize());
    }

    @Test
    public void testAndStartsWith() {
        Assert.assertEquals(s1b, f.and(s1a, s1b).optimize());
    }

    @Test
    public void testOrStartsWith() {
        Assert.assertEquals(s1a, f.or(s1a, s1b).optimize());
    }
}
