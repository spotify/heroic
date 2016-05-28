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
public class TagKeyCountTest extends AbstractReducedResultTest {
    private TagKeyCount s1;
    private TagKeyCount s2;
    private TagKeyCount s3;

    final TagKeyCount.Suggestion sug1 = new TagKeyCount.Suggestion("site", 5L);
    final TagKeyCount.Suggestion sug2 = new TagKeyCount.Suggestion("site", 5L);
    final TagKeyCount.Suggestion sug3 = new TagKeyCount.Suggestion("role", 10L);

    @Before
    public void setup() {
        s1 = new TagKeyCount(ImmutableList.of(e1), ImmutableList.of(sug1), false);
        s2 = new TagKeyCount(ImmutableList.of(e2), ImmutableList.of(sug2), false);
        s3 = new TagKeyCount(ImmutableList.of(), ImmutableList.of(sug3), false);
    }

    @Test
    public void reduceTest() throws Exception {
        assertEquals(new TagKeyCount(errors,
                ImmutableList.of(new TagKeyCount.Suggestion("role", 10L),
                    new TagKeyCount.Suggestion("site", 10L)), false),
            TagKeyCount.reduce(OptionalLimit.empty()).collect(ImmutableList.of(s1, s2, s3)));

        assertEquals(
            new TagKeyCount(errors, ImmutableList.of(new TagKeyCount.Suggestion("role", 10L)),
                true),
            TagKeyCount.reduce(OptionalLimit.of(1L)).collect(ImmutableList.of(s1, s2, s3)));
    }
}
