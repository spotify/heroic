package com.spotify.heroic.utils;

import java.util.List;
import java.util.Set;

import com.spotify.heroic.exceptions.BackendGroupException;

public interface GroupManager<T, G extends T> {
    public List<GroupMember<T>> getBackends();

    public G useDefaultGroup() throws BackendGroupException;

    public G useGroup(final String group) throws BackendGroupException;

    public G useGroups(final Set<String> groups) throws BackendGroupException;
}
