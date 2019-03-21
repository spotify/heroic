package com.spotify.heroic.suggest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.spotify.heroic.AbstractReducedResultTest;
import com.spotify.heroic.common.OptionalLimit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TagValuesSuggestTest extends AbstractReducedResultTest {
    private TagValuesSuggest s1;
    private TagValuesSuggest s2;
    private TagValuesSuggest s3;

    final TagValuesSuggest.Suggestion host =
        new TagValuesSuggest.Suggestion("host", ImmutableSortedSet.of("foo", "bar"), false);

    final TagValuesSuggest.Suggestion site =
        new TagValuesSuggest.Suggestion("site", ImmutableSortedSet.of("foo"), false);

    final TagValuesSuggest.Suggestion role =
        new TagValuesSuggest.Suggestion("role", ImmutableSortedSet.of("foo", "bar"), false);

    @Before
    public void setup() {
        s1 = new TagValuesSuggest(ImmutableList.of(e1), ImmutableList.of(site), false);
        s2 = new TagValuesSuggest(ImmutableList.of(e2), ImmutableList.of(host), false);
        s3 = new TagValuesSuggest(ImmutableList.of(), ImmutableList.of(role), false);
    }

    @Test
    public void reduceTest() throws Exception {
        assertEquals(new TagValuesSuggest(errors, ImmutableList.of(host, role, site), false),
            TagValuesSuggest
                .reduce(OptionalLimit.empty(), OptionalLimit.empty())
                .collect(ImmutableList.of(s1, s2, s3)));

        assertEquals(new TagValuesSuggest(errors, ImmutableList.of(
            new TagValuesSuggest.Suggestion(host.getKey(),
                ImmutableSortedSet.of(host.getValues().iterator().next()), true)), true),
            TagValuesSuggest
                .reduce(OptionalLimit.of(1L), OptionalLimit.of(1L))
                .collect(ImmutableList.of(s1, s2, s3)));
    }

    @Test
    public void suggestionSortTest() {
        assertEquals(ImmutableList.of(host, role, site),
            Ordering.natural().sortedCopy(ImmutableList.of(site, role, host)));
    }
}
