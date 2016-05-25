package com.spotify.heroic.filter;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class FilterTest {
    private final Filter a = new HasTagFilter("a");
    private final Filter b = new HasTagFilter("b");
    private final Filter c = new HasTagFilter("c");
    private final Filter tag1a = new MatchTagFilter("foo", "a");
    private final Filter tag1b = new MatchTagFilter("foo", "b");

    private final Filter s1a = new StartsWithFilter("foo", "abc");
    private final Filter s1b = new StartsWithFilter("foo", "abcdef");

    /**
     * {@link com.spotify.heroic.filter.Filter.Visitor} methods should all defer to {@link
     * com.spotify.heroic.filter.Filter.Visitor#defaultAction(Filter)} unless defined.
     */
    @Test
    public void visitorDefaultActionTest() {
        final Object ref = new Object();

        final Filter.Visitor<Object> v = filter -> ref;

        assertEquals(ref, v.visitStartsWith(Mockito.mock(StartsWithFilter.class)));
        assertEquals(ref, v.visitHasTag(Mockito.mock(HasTagFilter.class)));
        assertEquals(ref, v.visitNot(Mockito.mock(NotFilter.class)));
        assertEquals(ref, v.visitTrue(Mockito.mock(TrueFilter.class)));
        assertEquals(ref, v.visitFalse(Mockito.mock(FalseFilter.class)));
        assertEquals(ref, v.visitMatchTag(Mockito.mock(MatchTagFilter.class)));
        assertEquals(ref, v.visitMatchKey(Mockito.mock(MatchKeyFilter.class)));
        assertEquals(ref, v.visitAnd(Mockito.mock(AndFilter.class)));
        assertEquals(ref, v.visitOr(Mockito.mock(OrFilter.class)));
        assertEquals(ref, v.visitRaw(Mockito.mock(RawFilter.class)));
        assertEquals(ref, v.visitRegex(Mockito.mock(RegexFilter.class)));
    }

    @Test
    public void optimizeAndTest() {
        final Filter ref = AndFilter.of(a, b).optimize();
        Assert.assertTrue(ref instanceof Filter.MultiArgs);
        assertEquals(2, ((Filter.MultiArgs<?>) ref).terms().size());
        assertEquals(ref, AndFilter.of(a, b, b).optimize());
        assertEquals(ref, AndFilter.of(b, a).optimize());
        assertEquals(ref, AndFilter.of(b, AndFilter.of(a, b)).optimize());
    }

    @Test
    public void optimizeOrTest() {
        final Filter ref = OrFilter.of(a, b).optimize();
        Assert.assertTrue(ref instanceof Filter.MultiArgs);
        assertEquals(2, ((Filter.MultiArgs<?>) ref).terms().size());
        assertEquals(ref, OrFilter.of(a, b, b).optimize());
        assertEquals(ref, OrFilter.of(b, a).optimize());
        assertEquals(ref, OrFilter.of(b, OrFilter.of(a, b)).optimize());
    }

    @Test
    public void testSortOrder() {
        final List<List<Filter>> inputs =
            ImmutableList.<List<Filter>>of(ImmutableList.of(a, b, c), ImmutableList.of(c, b, a),
                ImmutableList.of(c, b, a));

        final List<Filter> reference = ImmutableList.of(a, b, c);

        for (final List<Filter> input : inputs) {
            final List<Filter> sorted = new ArrayList<>(new TreeSet<>(input));
            assertEquals(reference, sorted);
        }
    }

    @Test
    public void testOrFlatten() {
        assertEquals(OrFilter.of(a, b, c), OrFilter.of(a, OrFilter.of(b, c)).optimize());

        assertEquals(OrFilter.of(a, b, c),
            OrFilter.of(a, OrFilter.of(b, OrFilter.of(c))).optimize());

        assertEquals(OrFilter.of(a, b, c), OrFilter
            .of(a, NotFilter.of(AndFilter.of(NotFilter.of(b), NotFilter.of(c))))
            .optimize());
    }

    @Test
    public void testAndFlatten() {
        assertEquals(AndFilter.of(a, b, c), AndFilter.of(a, AndFilter.of(b, c)).optimize());

        assertEquals(AndFilter.of(a, b, c),
            AndFilter.of(a, AndFilter.of(b, AndFilter.of(c))).optimize());

        assertEquals(AndFilter.of(a, b, c), AndFilter
            .of(a, NotFilter.of(OrFilter.of(NotFilter.of(b), NotFilter.of(c))))
            .optimize());
    }

    @Test
    public void optimizeNotNot() {
        assertEquals(a, new NotFilter(new NotFilter(a)).optimize());
    }

    /**
     * The same expression, but inverted cannot co-exist in the same and statement.
     */
    @Test
    public void testAndContradiction() throws Exception {
        // same filter inverted
        assertEquals(FalseFilter.get(), AndFilter.of(a, new NotFilter(a)).optimize());
        // same tag with different values
        assertEquals(FalseFilter.get(), AndFilter.of(tag1a, tag1b).optimize());
    }

    /**
     * The same expression, but inverted cannot co-exist in the same and statement.
     */
    @Test
    public void testOrTautology() throws Exception {
        assertEquals(TrueFilter.get(), OrFilter.of(a, new NotFilter(a)).optimize());
    }

    @Test
    public void testAndStartsWith() {
        assertEquals(s1b, AndFilter.of(s1a, s1b).optimize());
        assertEquals(s1b, AndFilter.of(s1b, s1a).optimize());
    }

    @Test
    public void testOrStartsWith() {
        assertEquals(s1a, OrFilter.of(s1a, s1b).optimize());
        assertEquals(s1a, OrFilter.of(s1b, s1a).optimize());
    }
}
