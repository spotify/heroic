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

package com.spotify.heroic.suggest.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.DynamicModuleId;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.ModuleId;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.suggest.SuggestModule;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.Data;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

@Data
@ModuleId("elasticsearch")
public final class MemorySuggestModule implements SuggestModule, DynamicModuleId {
    public static final String DEFAULT_GROUP = "memory";

    private final Optional<String> id;
    private final Groups groups;

    @JsonCreator
    public MemorySuggestModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("groups") Optional<Groups> groups
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, final String id) {
        return DaggerMemorySuggestModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M())
            .build();
    }

    @MemoryScope
    @Component(modules = M.class,
        dependencies = {PrimaryComponent.class, Depends.class})
    interface C extends Exposed {
        @Override
        MemoryBackend backend();
    }

    @Module
    class M {
        @Provides
        @MemoryScope
        public Groups groups() {
            return groups;
        }
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> id = empty();
        private Optional<Groups> groups = empty();

        public Builder id(final String id) {
            checkNotNull(id, "id");
            this.id = of(id);
            return this;
        }

        public Builder group(final Groups groups) {
            checkNotNull(groups, "groups");
            this.groups = of(groups);
            return this;
        }

        public MemorySuggestModule build() {
            return new MemorySuggestModule(id, groups);
        }
    }
}
