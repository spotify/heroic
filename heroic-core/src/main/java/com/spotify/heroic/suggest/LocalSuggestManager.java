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

package com.spotify.heroic.suggest;

import com.spotify.heroic.common.GroupSet;
import eu.toolchain.async.AsyncFramework;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;

@SuggestScope
public class LocalSuggestManager implements SuggestManager {
    private final AsyncFramework async;
    private final GroupSet<SuggestBackend> groupSet;

    @Inject
    public LocalSuggestManager(
        final AsyncFramework async, @Named("groupSet") final GroupSet<SuggestBackend> groupSet
    ) {
        this.async = async;
        this.groupSet = groupSet;
    }

    @Override
    public GroupSet<SuggestBackend> groupSet() {
        return groupSet;
    }

    @Override
    public SuggestBackend useOptionalGroup(final Optional<String> group) {
        return new SuggestBackendGroup(async, groupSet.useOptionalGroup(group));
    }
}
