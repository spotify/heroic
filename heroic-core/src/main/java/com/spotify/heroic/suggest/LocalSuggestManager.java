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

import com.spotify.heroic.common.BackendGroups;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import eu.toolchain.async.AsyncFramework;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Set;

@SuggestScope
public class LocalSuggestManager implements SuggestManager {
    private final AsyncFramework async;
    private final BackendGroups<SuggestBackend> backends;
    private final LocalMetadataManagerReporter reporter;

    @Inject
    public LocalSuggestManager(
        final AsyncFramework async, @Named("backends") final BackendGroups<SuggestBackend> backends,
        final LocalMetadataManagerReporter reporter
    ) {
        this.async = async;
        this.backends = backends;
        this.reporter = reporter;
    }

    @Override
    public List<SuggestBackend> allMembers() {
        return backends.allMembers();
    }

    @Override
    public List<SuggestBackend> use(String group) {
        return backends.use(group).getMembers();
    }

    @Override
    public List<GroupMember<SuggestBackend>> getBackends() {
        return backends.all();
    }

    @Override
    public SuggestBackend useDefaultGroup() {
        return new SuggestBackendGroup(async, backends.useDefault(), reporter);
    }

    @Override
    public SuggestBackend useGroup(String group) {
        return new SuggestBackendGroup(async, backends.use(group), reporter);
    }

    @Override
    public SuggestBackend useGroups(Set<String> groups) {
        return new SuggestBackendGroup(async, backends.use(groups), reporter);
    }
}
