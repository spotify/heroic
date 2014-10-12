package com.spotify.heroic.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.google.common.collect.ImmutableList;

/**
 * Helper class to manage and query groups of backends.
 *
 * @author udoprog
 * @param <T>
 */
@Data
public class BackendGroups<T> {
    private final Map<String, List<T>> groups;
    private final List<T> defaults;

    public List<T> find(Set<String> groups) {
        final List<T> result = new ArrayList<>();

        for (final String group : groups) {
            final List<T> partial = find(group);

            if (partial == null)
                continue;

            result.addAll(partial);
        }

        if (result.isEmpty())
            return null;

        return ImmutableList.copyOf(result);
    }

    public List<T> find(String group) {
        if (group == null)
            return null;

        final List<T> result = groups.get(group);

        if (result == null || result.isEmpty())
            return null;

        return ImmutableList.copyOf(result);
    }

    public List<T> defaults() {
        return defaults;
    }

    public List<T> all() {
        final List<T> result = new ArrayList<>();

        for (final Map.Entry<String, List<T>> entry : groups.entrySet())
            result.addAll(entry.getValue());

        return ImmutableList.copyOf(result);
    }

    private static <T extends Grouped> Map<String, List<T>> buildBackends(Collection<T> backends) {
        final Map<String, List<T>> groups = new HashMap<>();

        for (final T backend : backends) {
            List<T> group = groups.get(backend.getGroup());

            if (group == null) {
                group = new ArrayList<>();
                groups.put(backend.getGroup(), group);
            }

            group.add(backend);
        }

        return groups;
    }

    private static <T extends Grouped> List<T> buildDefaults(final Map<String, List<T>> backends,
            Collection<String> defaultBackends) {
        final List<T> defaults = new ArrayList<>();

        // add all as defaults.
        if (defaultBackends == null) {
            for (final Map.Entry<String, List<T>> entry : backends.entrySet())
                defaults.addAll(entry.getValue());

            return defaults;
        }

        for (final String defaultBackend : defaultBackends) {
            final List<T> someResult = backends.get(defaultBackend);

            if (someResult == null)
                throw new IllegalArgumentException("No backend(s) available with id : " + defaultBackend);

            defaults.addAll(someResult);
        }

        return defaults;
    }

    public static <T extends Grouped> BackendGroups<T> build(Collection<T> configured,
            Collection<String> defaultBackends) {
        final Map<String, List<T>> backends = buildBackends(configured);
        final List<T> defaults = buildDefaults(backends, defaultBackends);
        return new BackendGroups<T>(backends, defaults);
    }
}
