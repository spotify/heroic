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

import com.spotify.heroic.common.BackendGroups;
import com.spotify.heroic.common.GroupMember;
import eu.toolchain.async.AsyncFramework;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;

@MetadataScope
public class LocalMetadataManager implements MetadataManager {
    private final AsyncFramework async;
    private final BackendGroups<MetadataBackend> backends;

    @Inject
    public LocalMetadataManager(
        final AsyncFramework async, @Named("backends") final BackendGroups<MetadataBackend> backends
    ) {
        this.async = async;
        this.backends = backends;
    }

    @Override
    public List<MetadataBackend> useMembers(final String group) {
        return backends.useGroup(group).getMembers();
    }

    @Override
    public List<MetadataBackend> useDefaultMembers() {
        return backends.useDefaultGroup().getMembers();
    }

    @Override
    public List<MetadataBackend> allMembers() {
        return backends.allMembers();
    }

    @Override
    public List<GroupMember<MetadataBackend>> getMembers() {
        return backends.all();
    }

    @Override
    public MetadataBackend useOptionalGroup(Optional<String> group) {
        return new MetadataBackendGroup(backends.useOptionalGroup(group), async);
    }
}
