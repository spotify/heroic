package com.spotify.heroic.suggest;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.AbstractReducedResultTest;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.test.LombokDataTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TagSuggestTest extends AbstractReducedResultTest {
    private TagSuggest s1;
    private TagSuggest s2;
    private TagSuggest s3;

    final TagSuggest.Suggestion site1 = new TagSuggest.Suggestion(1.0F, "site", "large");
    final TagSuggest.Suggestion site2 = new TagSuggest.Suggestion(0.5F, "site", "large");
    final TagSuggest.Suggestion role = new TagSuggest.Suggestion(2.0F, "role", "database");

    @Before
    public void setup() {
        s1 = new TagSuggest(ImmutableList.of(e1), ImmutableList.of(site1));
        s2 = new TagSuggest(ImmutableList.of(e2), ImmutableList.of(site2));
        s3 = new TagSuggest(ImmutableList.of(), ImmutableList.of(role));
    }

    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(TagSuggest.class);
    }

    @Test
    public void reduceTest() throws Exception {
        assertEquals(new TagSuggest(errors, ImmutableList.of(role, site1)),
            TagSuggest.reduce(OptionalLimit.empty()).collect(ImmutableList.of(s1, s2, s3)));

        assertEquals(new TagSuggest(errors, ImmutableList.of(role)),
            TagSuggest.reduce(OptionalLimit.of(1L)).collect(ImmutableList.of(s1, s2, s3)));
    }
}
