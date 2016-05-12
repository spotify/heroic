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

import java.util.Set;

/**
 * Container for a set of Grouped objects.
 * <p>
 * These grouped objects can be combined to form a uniform group of the same type (e.g. through
 * {@link #useGroups(java.util.Set)}.
 *
 * @param <G> The type of the grouped objects.
 */
public interface UsableGroupManager<G extends Grouped> {
    /**
     * Use the default group.
     *
     * @return The default group.
     */
    G useDefaultGroup();

    /**
     * Use the given group.
     *
     * @param group The name of the group to use.
     * @return The group corresponding to the name.
     */
    G useGroup(final String group);

    /**
     * Use the the given group, based on a set of groups.
     *
     * @param groups The names of the groups to use.
     * @return A group corresponding to a combination of all the names.
     */
    G useGroups(final Set<String> groups);
}
