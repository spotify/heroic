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

import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.NoArgsConstructor;

import com.spotify.heroic.exceptions.BackendGroupException;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.utils.BackendGroups;
import com.spotify.heroic.utils.GroupMember;

import eu.toolchain.async.AsyncFramework;

@NoArgsConstructor
public class LocalSuggestManager implements SuggestManager {
    @Inject
    private AsyncFramework async;

    @Inject
    @Named("backends")
    private BackendGroups<SuggestBackend> backends;

    @Inject
    private LocalMetadataManagerReporter reporter;

    @Override
    public List<GroupMember<SuggestBackend>> getBackends() {
        return backends.all();
    }

    @Override
    public SuggestBackend useDefaultGroup() throws BackendGroupException {
        return new SuggestBackendGroup(async, backends.useDefault(), reporter);
    }

    @Override
    public SuggestBackend useGroup(String group) throws BackendGroupException {
        return new SuggestBackendGroup(async, backends.use(group), reporter);
    }

    @Override
    public SuggestBackend useGroups(Set<String> groups) throws BackendGroupException {
        return new SuggestBackendGroup(async, backends.use(groups), reporter);
    }
}