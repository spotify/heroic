package com.spotify.heroic.suggest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metric.RequestError;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TagSuggestTest {
    @Mock
    private RequestError e1;

    @Mock
    private RequestError e2;

    private TagSuggest s1;
    private TagSuggest s2;
    private TagSuggest s3;

    final TagSuggest.Suggestion site1 = new TagSuggest.Suggestion(1.0F, "site", "large");
    final TagSuggest.Suggestion site2 = new TagSuggest.Suggestion(0.5F, "site", "large");
    final TagSuggest.Suggestion role = new TagSuggest.Suggestion(2.0F, "role", "database");

    @Before
    public void setup() {
        s1 = new TagSuggest(ImmutableList.of(e1), ImmutableSortedSet.of(site1));
        s2 = new TagSuggest(ImmutableList.of(e2), ImmutableSortedSet.of(site2));
        s3 = new TagSuggest(ImmutableList.of(), ImmutableSortedSet.of(role));
    }

    @Test
    public void reduceTest() throws Exception {
        final TagSuggest ref =
            new TagSuggest(ImmutableList.of(e1, e2), ImmutableSortedSet.of(site1, role));

        final TagSuggest result =
            TagSuggest.reduce(OptionalLimit.empty()).collect(ImmutableList.of(s1, s2, s3));

        assertEquals(ref, result);
    }

    @Test
    public void reduceLimitedTest() throws Exception {
        final TagSuggest ref =
            new TagSuggest(ImmutableList.of(e1, e2), ImmutableSortedSet.of(role));

        final TagSuggest result =
            TagSuggest.reduce(OptionalLimit.of(1L)).collect(ImmutableList.of(s1, s2, s3));

        assertEquals(ref, result);
    }
}
