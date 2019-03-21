package com.spotify.heroic.suggest;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.AbstractReducedResultTest;
import com.spotify.heroic.common.OptionalLimit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TagValueSuggestTest extends AbstractReducedResultTest {
    private TagValueSuggest s1;
    private TagValueSuggest s2;
    private TagValueSuggest s3;

    @Before
    public void setup() {
        s1 = new TagValueSuggest(ImmutableList.of(e1), ImmutableList.of("foo", "bar"), false);
        s2 = new TagValueSuggest(ImmutableList.of(e2), ImmutableList.of("foo"), false);
        s3 = new TagValueSuggest(ImmutableList.of(), ImmutableList.of("foo", "baz"), false);
    }

    @Test
    public void reduceTest() throws Exception {
        assertEquals(new TagValueSuggest(errors, ImmutableList.of("bar", "baz", "foo"), false),
            TagValueSuggest.reduce(OptionalLimit.empty()).collect(ImmutableList.of(s1, s2, s3)));

        assertEquals(new TagValueSuggest(errors, ImmutableList.of("bar", "baz"), true),
            TagValueSuggest.reduce(OptionalLimit.of(2L)).collect(ImmutableList.of(s1, s2, s3)));
    }
}
