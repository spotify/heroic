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

package com.spotify.heroic.metric.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricModule;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import lombok.Data;

import javax.inject.Named;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@Data
public final class MemoryMetricModule implements MetricModule {
    public static final String DEFAULT_GROUP = "memory";

    private final Optional<String> id;
    private final Groups groups;
    private final boolean synchronizedStorage;

    @JsonCreator
    public MemoryMetricModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("synchronizedStorage") Optional<Boolean> synchronizedStorage
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.synchronizedStorage = synchronizedStorage.orElse(false);
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, String id) {
        return DaggerMemoryMetricModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M())
            .build();
    }

    @MemoryScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class, Depends.class})
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

        @Provides
        @MemoryScope
        @Named("storage")
        public Map<MemoryBackend.MemoryKey, NavigableMap<Long, Metric>> metricBackend(
            final AsyncFramework async
        ) {
            if (synchronizedStorage) {
                return Collections.synchronizedMap(new HashMap<>());
            }

            return new ConcurrentSkipListMap<>(MemoryBackend.COMPARATOR);
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

        public Builder id(String id) {
            this.id = of(id);
            return this;
        }

        public Builder groups(Groups groups) {
            this.groups = of(groups);
            return this;
        }

        public Builder synchronizedStorage(final boolean synchronizedStorage) {
            this.synchronizedStorage = of(synchronizedStorage);
            return this;
        }

        public MemoryMetricModule build() {
            return new MemoryMetricModule(id, groups, synchronizedStorage);
        }
    }
}
