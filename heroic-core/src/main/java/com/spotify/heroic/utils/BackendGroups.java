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

package com.spotify.heroic.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.exceptions.BackendGroupException;

/**
 * Helper class to manage and query groups of backends.
 *
 * @author udoprog
 * @param <T>
 */
@Data
public class BackendGroups<T extends Initializing & Grouped> {
    private final Map<String, List<T>> groups;
    private final List<T> defaults;

    /**
     * Use default groups and guarantee that at least one is available.
     */
    public SelectedGroup<T> useDefault() throws BackendGroupException {
        return filterAlive(defaults());
    }

    /**
     * Use the given group and guarantee that at least one is available.
     */
    public SelectedGroup<T> use(final String group) throws BackendGroupException {
        return filterAlive(group != null ? find(group) : defaults());
    }

    /**
     * Use the given groups and guarantee that at least one is available.
     */
    public SelectedGroup<T> use(final Set<String> groups) throws BackendGroupException {
        return filterAlive(groups != null ? find(groups) : defaults());
    }

    public List<T> defaults() {
        return defaults;
    }

    public List<GroupMember<T>> all() {
        final List<GroupMember<T>> result = new ArrayList<>();

        for (final Map.Entry<String, List<T>> entry : groups.entrySet()) {
            for (final T e : entry.getValue()) {
                result.add(new GroupMember<>(e, e.getGroups(), defaults.contains(e)));
            }
        }

        return ImmutableList.copyOf(result);
    }

    public T findOne(String group) {
        final List<T> results = group != null ? find(group) : defaults();

        if (results.isEmpty())
            throw new IllegalArgumentException("Could not find one member of group '" + group + "'");

        return results.iterator().next();
    }

    private List<T> find(Set<String> groups) {
        final List<T> result = new ArrayList<>();

        for (final String group : groups)
            result.addAll(find(group));

        return ImmutableList.copyOf(result);
    }

    private List<T> find(String group) {
        if (group == null)
            throw new IllegalArgumentException("group");

        final List<T> result = groups.get(group);

        if (result == null || result.isEmpty())
            return ImmutableList.of();

        return ImmutableList.copyOf(result);
    }

    private SelectedGroup<T> filterAlive(List<T> backends) throws BackendGroupException {
        final List<T> alive = new ArrayList<T>();

        // Keep track of groups which are not ready.
        int disabled = 0;

        for (final T backend : backends) {
            if (!backend.isReady()) {
                ++disabled;
                continue;
            }

            alive.add(backend);
        }

        return new SelectedGroup<T>(disabled, alive);
    }

    private static <T extends Grouped> Map<String, List<T>> buildBackends(Collection<T> backends) {
        final Map<String, List<T>> groups = new HashMap<>();

        for (final T backend : backends) {
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
                throw new IllegalArgumentException("No backend(s) available with group: " + defaultBackend);

            defaults.addAll(someResult);
        }

        return defaults;
    }

    public static <T extends Grouped & Initializing> BackendGroups<T> build(Collection<T> configured,
            Collection<String> defaultBackends) {
        final Map<String, List<T>> backends = buildBackends(configured);
        final List<T> defaults = buildDefaults(backends, defaultBackends);
        return new BackendGroups<T>(backends, defaults);
    }
}
