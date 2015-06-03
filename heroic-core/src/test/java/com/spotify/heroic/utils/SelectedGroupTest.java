package com.spotify.heroic.utils;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

public class SelectedGroupTest {
    private final Set<String> groupA = ImmutableSet.of("foo", "bar");
    private final Set<String> groupB = ImmutableSet.of("bar", "baz");

    @Test
    public void testMergedGroups() {
        final Grouped a = Mockito.mock(Grouped.class);
        final Grouped b = Mockito.mock(Grouped.class);
        Mockito.when(a.getGroups()).thenReturn(groupA);
        Mockito.when(b.getGroups()).thenReturn(groupB);

        final SelectedGroup<Grouped> g = new SelectedGroup<>(0, ImmutableList.of(a, b));

        Assert.assertEquals(ImmutableSortedSet.of("bar", "baz", "foo"), ImmutableSortedSet.copyOf(g.groups()));
    }
}