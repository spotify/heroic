package com.spotify.heroic.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.SelectedGroup;
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
        Mockito.when(a.getGroups()).thenReturn(groupA);
        Mockito.when(b.getGroups()).thenReturn(groupB);

        final SelectedGroup<Grouped> g = new SelectedGroup<>(ImmutableList.of(a, b));

        Assert.assertEquals(ImmutableSortedSet.of("bar", "baz", "foo"),
            ImmutableSortedSet.copyOf(g.groups()));
    }
}
