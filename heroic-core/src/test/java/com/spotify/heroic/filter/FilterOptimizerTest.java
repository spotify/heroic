package com.spotify.heroic.filter;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class FilterOptimizerTest {
    private final Filter a = new HasTagFilter("a");
    private final Filter b = new HasTagFilter("b");
    private final Filter c = new HasTagFilter("c");
    private final Filter tag1a = new MatchTagFilter("foo", "a");
    private final Filter tag1b = new MatchTagFilter("foo", "b");

    private final Filter s1a = new StartsWithFilter("foo", "abc");
    private final Filter s1b = new StartsWithFilter("foo", "abcdef");

    @Test
    public void optimizeAndTest() {
        final Filter ref = AndFilter.of(a, b).optimize();
        Assert.assertTrue(ref instanceof Filter.MultiArgs);
        Assert.assertEquals(2, ((Filter.MultiArgs<?>) ref).terms().size());
        Assert.assertEquals(ref, AndFilter.of(a, b, b).optimize());
        Assert.assertEquals(ref, AndFilter.of(b, a).optimize());
        Assert.assertEquals(ref, AndFilter.of(b, AndFilter.of(a, b)).optimize());
    }

    @Test
    public void optimizeOrTest() {
        final Filter ref = OrFilter.of(a, b).optimize();
        Assert.assertTrue(ref instanceof Filter.MultiArgs);
        Assert.assertEquals(2, ((Filter.MultiArgs<?>) ref).terms().size());
        Assert.assertEquals(ref, OrFilter.of(a, b, b).optimize());
        Assert.assertEquals(ref, OrFilter.of(b, a).optimize());
        Assert.assertEquals(ref, OrFilter.of(b, OrFilter.of(a, b)).optimize());
    }

    @Test
    public void testSortOrder() {
        final List<List<Filter>> inputs =
            ImmutableList.<List<Filter>>of(ImmutableList.of(a, b, c), ImmutableList.of(c, b, a),
                ImmutableList.of(c, b, a));

        final List<Filter> reference = ImmutableList.of(a, b, c);

        for (final List<Filter> input : inputs) {
            final List<Filter> sorted = new ArrayList<>(new TreeSet<>(input));
            Assert.assertEquals(reference, sorted);
        }
    }

    @Test
    public void testOrFlatten() {
        Assert.assertEquals(OrFilter.of(a, b, c), OrFilter.of(a, OrFilter.of(b, c)).optimize());
    }

    @Test
    public void testAndFlatten() {
        Assert.assertEquals(AndFilter.of(a, b, c), AndFilter.of(a, AndFilter.of(b, c)).optimize());
    }

    @Test
    public void optimizeNotNot() {
        Assert.assertEquals(a, new NotFilter(new NotFilter(a)).optimize());
    }

    /**
     * The same expression, but inverted cannot co-exist in the same and statement.
     */
    @Test
    public void testAndContradiction1() throws Exception {
        Assert.assertEquals(FalseFilter.get(), AndFilter.of(a, new NotFilter(a)).optimize());
    }

    /**
     * A match tag, with two different value conditions cannot co-exist in the same and statement.
     */
    @Test
    public void testAndContradiction2() throws Exception {
        Assert.assertEquals(FalseFilter.get(), AndFilter.of(tag1a, tag1b).optimize());
    }

    /**
     * The same expression, but inverted cannot co-exist in the same and statement.
     */
    @Test
    public void testOrTautology1() throws Exception {
        Assert.assertEquals(TrueFilter.get(), OrFilter.of(a, new NotFilter(a)).optimize());
    }

    @Test
    public void testAndStartsWith() {
        Assert.assertEquals(s1b, AndFilter.of(s1a, s1b).optimize());
    }

    @Test
    public void testOrStartsWith() {
        Assert.assertEquals(s1a, OrFilter.of(s1a, s1b).optimize());
    }
}
