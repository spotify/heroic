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

package com.spotify.heroic.metadata.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataModule;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

@Data
public final class MemoryMetadataModule implements MetadataModule {
    public static final String DEFAULT_GROUP = "memory";

    private final Optional<String> id;
    private final Groups groups;
    private final boolean synchronizedStorage;

    @JsonCreator
    public MemoryMetadataModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("synchronizedStorage") Optional<Boolean> synchronizedStorage
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.synchronizedStorage = synchronizedStorage.orElse(false);
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, final String id) {
        return DaggerMemoryMetadataModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M(synchronizedStorage, groups))
            .build();
    }

    @MemoryScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class, Depends.class})
    interface C extends Exposed {
        @Override
        MetadataBackend backend();
    }

    @RequiredArgsConstructor
    @Module
    class M {
        private final boolean synchronizedStorage;
        private final Groups groups;

        @MemoryScope
        @Provides
        public MetadataBackend backend(AsyncFramework async) {
            final Set<Series> storage;

            if (synchronizedStorage) {
                storage = Collections.synchronizedSet(new HashSet<>());
            } else {
                storage = new ConcurrentSkipListSet<>();
            }

            return new MemoryBackend(async, groups, storage);
        }
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("memory#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> id = empty();
        private Optional<Groups> groups = empty();
        private Optional<Boolean> synchronizedStorage = empty();

        public Builder id(final String id) {
            checkNotNull(id, "id");
            this.id = of(id);
            return this;
        }

        public Builder groups(final Groups groups) {
            checkNotNull(groups, "groups");
            this.groups = of(groups);
            return this;
        }

        public Builder synchronizedStorage(final boolean synchronizedStorage) {
            this.synchronizedStorage = of(synchronizedStorage);
            return this;
        }

        public MemoryMetadataModule build() {
            return new MemoryMetadataModule(id, groups, synchronizedStorage);
        }
    }
}
