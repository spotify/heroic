package com.spotify.heroic.common;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class GroupSetTest {
    final Grouped a = grouped("foo");
    final Grouped b = grouped("foo", "bar");
    final Grouped c = grouped("bar");

    private final GroupSet<Grouped> g =
        GroupSet.build(ImmutableSet.of(a, b, c), Optional.empty());

    @Test
    public void useGroupTest() {
        assertEquals(ImmutableSet.of(a, b, c), g.useDefaultGroup().getMembers());
        assertEquals(ImmutableSet.of(a, b, c), g.useOptionalGroup(Optional.empty()).getMembers());
        assertEquals(ImmutableSet.of(a, b), g.useGroup("foo").getMembers());
        assertEquals(ImmutableSet.of(b, c), g.useGroup("bar").getMembers());
    }

    @Test
    public void useMissingGroupTest() {
        assertEquals(ImmutableSet.of(), g.useGroup("baz").getMembers());
    }

    @Test(expected = NullPointerException.class)
    public void useNullFailureTest() {
        assertEquals(ImmutableSet.of(), g.useGroup(null).getMembers());
    }

    static Grouped grouped(final String... names) {
        final Set<String> groups = ImmutableSet.copyOf(names);
        final Groups g = new Groups(groups);

        return new Grouped() {
            @Override
            public Groups groups() {
                return g;
            }
        };
    }
}
