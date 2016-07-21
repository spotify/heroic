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

import java.util.Optional;

/**
 * Container for a set of Grouped objects.
 *
 * @param <G> The type of the grouped objects.
 */
public interface UsableGroupManager<G> {
    /**
     * Use the default group.
     *
     * @return The default group.
     */
    default G useDefaultGroup() {
        return useOptionalGroup(Optional.empty());
    }

    /**
     * Use the given group.
     *
     * @param group The name of the group to use. Must not be <code>null</code>.
     * @return The group corresponding to the name.
     */
    default G useGroup(final String group) {
        // TODO: get rid of all null usages
        return useOptionalGroup(Optional.ofNullable(group));
    }

    /**
     * Use the given group based on an {@link java.util.Optional} parameter.
     * <p>
     * If it is <em>empty</em>, this will resolve the same as {@link #useDefaultGroup()}, otherwise
     * the provided value will be used with {@link #useGroup(String)}.
     *
     * @param group The optional name of the group to use.
     * @return The group corresponding to the optional name.
     */
    G useOptionalGroup(final Optional<String> group);
}
