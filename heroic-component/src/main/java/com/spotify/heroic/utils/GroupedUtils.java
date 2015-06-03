package com.spotify.heroic.utils;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public final class GroupedUtils {
    public static Set<String> groups(String group, Set<String> groups, String defaultGroup) {
        if (group != null)
            return ImmutableSet.of(group);

        if (groups != null)
            return groups;

        return ImmutableSet.of(defaultGroup);
    }
}
