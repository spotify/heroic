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

import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Helper class to manage and query groups of backends.
 *
 * @param <T>
 * @author udoprog
 */
@RequiredArgsConstructor
public class BackendGroups<T extends Grouped> implements UsableGroupManager<SelectedGroup<T>> {
    private final List<T> allMembers;
    private final Map<String, List<T>> groups;
    private final List<T> defaults;

    public List<T> allMembers() {
        return allMembers;
    }

    /**
     * Use default groups and guarantee that at least one is available.
     */
    @Override
    public SelectedGroup<T> useDefaultGroup() {
        return selected(defaults);
    }

    @Override
    public SelectedGroup<T> useGroup(final String group) {
        Objects.requireNonNull(group, "group must not be null");
        return selected(find(group));
    }

    @Override
    public SelectedGroup<T> useOptionalGroup(final Optional<String> group) {
        return group.map(this::useGroup).orElseGet(this::useDefaultGroup);
    }

    public List<GroupMember<T>> all() {
        final List<GroupMember<T>> result = new ArrayList<>();

        for (final Map.Entry<String, List<T>> entry : groups.entrySet()) {
            for (final T e : entry.getValue()) {
                result.add(
                    new GroupMember<>(entry.getKey(), e, e.getGroups(), defaults.contains(e)));
            }
        }

        return ImmutableList.copyOf(result);
    }

    private List<T> find(String group) {
        final List<T> result = groups.get(group);

        if (result == null || result.isEmpty()) {
            return ImmutableList.of();
        }

        return ImmutableList.copyOf(result);
    }

    private SelectedGroup<T> selected(final List<T> backends) {
        return new SelectedGroup<>(backends);
    }

    private static <T extends Grouped> Map<String, List<T>> buildBackends(Collection<T> backends) {
        final Map<String, List<T>> groups = new HashMap<>();

        for (final T backend : backends) {
            if (backend.getGroups().isEmpty()) {
                throw new IllegalStateException(
                    "Backend " + backend + " does not belong to any groups");
            }

            for (final String name : backend.getGroups()) {
                List<T> group = groups.get(name);

                if (group == null) {
                    group = new ArrayList<>();
                    groups.put(name, group);
                }

                group.add(backend);
            }
        }

        return groups;
    }

    private static <T extends Grouped> Set<T> buildDefaults(
        final Map<String, List<T>> backends, Optional<List<String>> defaultBackends
    ) {
        final Set<T> defaults = new HashSet<>();

        // add all as defaults.
        if (!defaultBackends.isPresent()) {
            for (final Map.Entry<String, List<T>> entry : backends.entrySet()) {
                defaults.addAll(entry.getValue());
            }

            return defaults;
        }

        for (final String defaultBackend : defaultBackends.get()) {
            final List<T> someResult = backends.get(defaultBackend);

            if (someResult == null) {
                throw new IllegalArgumentException(
                    "No backend(s) available with group: " + defaultBackend);
            }

            defaults.addAll(someResult);
        }

        return defaults;
    }

    public static <T extends Grouped & Initializing> BackendGroups<T> build(
        Collection<T> configured, Optional<List<String>> defaultBackends
    ) {
        final Map<String, List<T>> mappings = buildBackends(configured);
        final Set<T> defaults = buildDefaults(mappings, defaultBackends);
        return new BackendGroups<T>(ImmutableList.copyOf(configured), mappings,
            ImmutableList.copyOf(defaults));
    }
}
