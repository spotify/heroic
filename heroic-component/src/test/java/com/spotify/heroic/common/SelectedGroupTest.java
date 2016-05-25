package com.spotify.heroic.common;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SelectedGroupTest {
    private final Groups groupA = new Groups(ImmutableSet.of("foo", "bar"));
    private final Groups groupB = new Groups(ImmutableSet.of("bar", "baz"));

    @Test
    public void testMergedGroups() {
        final Grouped a = Mockito.mock(Grouped.class);
        final Grouped b = Mockito.mock(Grouped.class);
        Mockito.when(a.groups()).thenReturn(groupA);
        Mockito.when(b.groups()).thenReturn(groupB);

        final SelectedGroup<Grouped> g = new SelectedGroup<>(ImmutableSet.of(a, b));

        Assert.assertEquals(ImmutableSortedSet.of("bar", "baz", "foo"),
            ImmutableSortedSet.copyOf(g.groups()));
    }
}
