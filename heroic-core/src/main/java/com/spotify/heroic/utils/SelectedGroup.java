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