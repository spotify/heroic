/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.common;

import com.google.common.collect.ImmutableSet;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Helper class to manager groups of {@link com.spotify.heroic.common.Grouped} objects and query
 * them.
 *
 * @param <T> The type of the grouped objects.
 * @author udoprog
 */
@RequiredArgsConstructor
public class GroupSet<T extends Grouped> implements UsableGroupManager<SelectedGroup<T>> {
    private final Set<T> all;
    private final Map<String, Set<T>> groups;
    private final Set<T> defaults;

    /**
     * All available group names.
     */
    public Set<String> names() {
        return groups.keySet();
    }

    /**
     * Use all members of this set.
     *
     * @return All members of this set.
     */
    public SelectedGroup<T> useAll() {
        return new SelectedGroup<>(all);
    }

    /**
     * Use the default group.
     *
     * @return The default group.
     */
    @Override
    public SelectedGroup<T> useDefaultGroup() {
        return new SelectedGroup<>(defaults);
    }

    /**
     * Use the specific group.
     *
     * @param group The name of the group to use. Must not be {@code null}.
     * @return The specified group.
     * @throws java.lang.NullPointerException if {@code group} is {@code null}.
     */
    @Override
    public SelectedGroup<T> useGroup(final String group) {
        Objects.requireNonNull(group, "group must not be null");
        return new SelectedGroup<>(find(group));
    }

    /**
     * Use the specific group, out of an {@link java.util.Optional} parameter.
     * <p>
     * This method behaves like {@link #useDefaultGroup()} if the optional is empty, otherwise it
     * behaves as {@link #useGroup(String)}. This method is mainly provided for convenience.
     *
     * @param group The optional name of the group to use.
     * @return The specified group.
     */
    @Override
    public SelectedGroup<T> useOptionalGroup(final Optional<String> group) {
        return group.map(this::useGroup).orElseGet(this::useDefaultGroup);
    }

    /**
     * Get all members of this set with some metadata in the form of {@link
     * com.spotify.heroic.common.GroupMember}.
     *
     * @return All members of this set.
     */
    public Set<GroupMember<T>> inspectAll() {
        final ImmutableSet.Builder<GroupMember<T>> result = ImmutableSet.builder();

        for (final Map.Entry<String, Set<T>> entry : groups.entrySet()) {
            for (final T e : entry.getValue()) {
                result.add(new GroupMember<>(entry.getKey(), e, e.groups(), defaults.contains(e)));
            }
        }

        return result.build();
    }

    private Set<T> find(String group) {
        final Set<T> result = groups.get(group);

        if (result == null) {
            return ImmutableSet.of();
        }

        return result;
    }

    private static <T extends Grouped> Map<String, Set<T>> buildBackends(Collection<T> backends) {
        final Map<String, Set<T>> groups = new HashMap<>();

        for (final T backend : backends) {
            if (backend.groups().isEmpty()) {
                throw new IllegalStateException(
                    "Backend " + backend + " does not belong to any groups");
            }

            for (final String name : backend.groups()) {
                Set<T> group = groups.get(name);

                if (group == null) {
                    group = new HashSet<>();
                    groups.put(name, group);
                }

                group.add(backend);
            }
        }

        return groups;
    }

    private static <T extends Grouped> Set<T> buildDefaults(
        final Map<String, Set<T>> backends, Optional<List<String>> defaultBackends
    ) {
        final Set<T> defaults = new HashSet<>();

        // add all as defaults.
        if (!defaultBackends.isPresent()) {
            for (final Map.Entry<String, Set<T>> entry : backends.entrySet()) {
                defaults.addAll(entry.getValue());
            }

            return defaults;
        }

        for (final String defaultBackend : defaultBackends.get()) {
            final Set<T> someResult = backends.get(defaultBackend);

            if (someResult == null) {
                throw new IllegalArgumentException(
                    "No backend(s) available with group: " + defaultBackend);
            }

            defaults.addAll(someResult);
        }

        return defaults;
    }

    public static <T extends Grouped> GroupSet<T> build(
        Collection<T> configured, Optional<List<String>> defaultBackends
    ) {
        final Map<String, Set<T>> mappings = buildBackends(configured);
        final Set<T> defaults = buildDefaults(mappings, defaultBackends);
        return new GroupSet<>(ImmutableSet.copyOf(configured), mappings,
            ImmutableSet.copyOf(defaults));
    }
}
