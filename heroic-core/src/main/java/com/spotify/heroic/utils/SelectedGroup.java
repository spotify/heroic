package com.spotify.heroic.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import lombok.Data;

@Data
public class SelectedGroup<T extends Grouped> implements Iterable<T> {
    private final int disabled;
    private final List<T> members;

    @Override
    public Iterator<T> iterator() {
        return members.iterator();
    }

    public boolean isEmpty() {
        return members.isEmpty();
    }

    public Set<String> groups() {
        final Set<String> groups = new HashSet<>();

        for (final T member : members) {
            groups.addAll(member.getGroups());
        }

        return groups;
    }
}