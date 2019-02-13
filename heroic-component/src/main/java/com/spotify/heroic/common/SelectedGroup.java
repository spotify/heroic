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

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public class SelectedGroup<T extends Grouped> implements Grouped, Iterable<T> {
    private final Set<T> members;

    public SelectedGroup(final Set<T> members) {
        this.members = members;
    }

    public Set<T> getMembers() {
        return members;
    }

    @Override
    public Iterator<T> iterator() {
        return members.iterator();
    }

    public Stream<T> stream() {
        return members.stream();
    }

    public boolean isEmpty() {
        return members.isEmpty();
    }

    @Override
    public Groups groups() {
        final ImmutableSet.Builder<String> groups = ImmutableSet.builder();

        for (final T member : members) {
            groups.addAll(member.groups());
        }

        return new Groups(groups.build());
    }

    public int size() {
        return members.size();
    }
}
