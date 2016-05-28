package com.spotify.heroic.suggest;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metric.RequestError;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TagValueSuggestTest {
    @Mock
    private RequestError e1;

    @Mock
    private RequestError e2;

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
        final TagValueSuggest ref =
            new TagValueSuggest(ImmutableList.of(e1, e2), ImmutableList.of("bar", "baz", "foo"),
                false);

        final TagValueSuggest result =
            TagValueSuggest.reduce(OptionalLimit.empty()).collect(ImmutableList.of(s1, s2, s3));

        assertEquals(ref, result);
    }

    @Test
    public void reduceLimitedTest() throws Exception {
        final TagValueSuggest ref =
            new TagValueSuggest(ImmutableList.of(e1, e2), ImmutableList.of("bar", "baz"), true);

        final TagValueSuggest result =
            TagValueSuggest.reduce(OptionalLimit.of(2L)).collect(ImmutableList.of(s1, s2, s3));

        assertEquals(ref, result);
    }
}
