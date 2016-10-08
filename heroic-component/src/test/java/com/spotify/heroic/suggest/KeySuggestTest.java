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
public class KeySuggestTest extends AbstractReducedResultTest {
    private KeySuggest s1;
    private KeySuggest s2;
    private KeySuggest s3;

    final KeySuggest.Suggestion sug1 = new KeySuggest.Suggestion(0.5F, "foo");
    final KeySuggest.Suggestion sug2 = new KeySuggest.Suggestion(1.0F, "foo");
    final KeySuggest.Suggestion sug3 = new KeySuggest.Suggestion(0.0F, "bar");

    @Before
    public void setup() {
        s1 = new KeySuggest(ImmutableList.of(e1), ImmutableList.of(sug1));
        s2 = new KeySuggest(ImmutableList.of(e2), ImmutableList.of(sug2));
        s3 = new KeySuggest(ImmutableList.of(), ImmutableList.of(sug3));
    }

    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(KeySuggest.class);
    }

    @Test
    public void reduceTest() throws Exception {
        assertEquals(new KeySuggest(errors, ImmutableList.of(sug2, sug3)),
            KeySuggest.reduce(OptionalLimit.empty()).collect(ImmutableList.of(s1, s2, s3)));

        assertEquals(new KeySuggest(errors, ImmutableList.of(sug2)),
            KeySuggest.reduce(OptionalLimit.of(1L)).collect(ImmutableList.of(s1, s2, s3)));
    }
}
