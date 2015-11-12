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

package com.spotify.heroic.metadata;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;

import com.spotify.heroic.common.BackendGroups;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;

import eu.toolchain.async.AsyncFramework;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class LocalMetadataManager implements MetadataManager {
    @Inject
    private AsyncFramework async;

    @Inject
    @Named("backends")
    private BackendGroups<MetadataBackend> backends;

    @Inject
    private LocalMetadataManagerReporter reporter;

    @Override
    public List<MetadataBackend> allMembers() {
        return backends.allMembers();
    }

    @Override
    public List<MetadataBackend> use(String group) {
        return backends.use(group).getMembers();
    }

    @Override
    public List<GroupMember<MetadataBackend>> getBackends() {
        return backends.all();
    }

    @Override
    public MetadataBackend useDefaultGroup() {
        return new MetadataBackendGroup(backends.useDefault(), async, reporter);
    }

    @Override
    public MetadataBackend useGroup(String group) {
        return new MetadataBackendGroup(backends.use(group), async, reporter);
    }

    @Override
    public MetadataBackend useGroups(Set<String> groups) {
        return new MetadataBackendGroup(backends.use(groups), async, reporter);
    }
}
