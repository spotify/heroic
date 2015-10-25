package com.spotify.heroic.common;

import java.util.Set;

public interface UsableGroupManager<G extends Grouped> {
    public G useDefaultGroup();

    public G useGroup(final String group);

    public G useGroups(final Set<String> groups);
}