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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.BackendGroups;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.spotify.heroic.common.Optionals.mergeOptionalList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

@RequiredArgsConstructor
public class SuggestManagerModule {
    private final List<SuggestModule> backends;
    private final Optional<List<String>> defaultBackends;

    public SuggestComponent module(final PrimaryComponent primary) {
        return DaggerSuggestManagerModule_C
            .builder()
            .primaryComponent(primary)
            .m(new M(primary))
            .build();
    }

    @SuggestScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class})
    interface C extends SuggestComponent {
        @Override
        LocalSuggestManager suggestManager();

        @Override
        @Named("suggest")
        LifeCycle suggestLife();
    }

    @RequiredArgsConstructor
    @Module
    class M {
        private final PrimaryComponent primary;

        @Provides
        @SuggestScope
        public LocalMetadataManagerReporter localReporter(HeroicReporter reporter) {
            return reporter.newLocalMetadataBackendManager();
        }

        @Provides
        @SuggestScope
        public ClusteredMetadataManagerReporter clusteredReporter(HeroicReporter reporter) {
            return reporter.newClusteredMetadataBackendManager();
        }

        @Provides
        @Named("backends")
        @SuggestScope
        public BackendGroups<SuggestBackend> defaultBackends(Set<SuggestBackend> configured) {
            return BackendGroups.build(configured, defaultBackends);
        }

        @Provides
        @SuggestScope
        public List<SuggestModule.Exposed> components(LocalMetadataManagerReporter reporter) {
            final ArrayList<SuggestModule.Exposed> results = new ArrayList<>();

            final AtomicInteger i = new AtomicInteger();

            for (final SuggestModule m : backends) {
                final String id = m.id().orElseGet(() -> m.buildId(i.getAndIncrement()));

                final SuggestModule.Depends depends =
                    new SuggestModule.Depends(reporter.newMetadataBackend(id));

                results.add(m.module(primary, depends, id));
            }

            return results;
        }

        @Provides
        @SuggestScope
        public Set<SuggestBackend> backends(List<SuggestModule.Exposed> components) {
            return ImmutableSet.copyOf(components.stream().map(c -> c.backend()).iterator());
        }

        @Provides
        @SuggestScope
        @Named("suggest")
        public LifeCycle suggestLife(List<SuggestModule.Exposed> components) {
            return LifeCycle.combined(components.stream().map(c -> c.life()));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Optional<List<SuggestModule>> backends = empty();
        private Optional<List<String>> defaultBackends = empty();

        @JsonCreator
        public Builder(
            @JsonProperty("backends") List<SuggestModule> backends,
            @JsonProperty("defaultBackends") List<String> defaultBackends
        ) {
            this.backends = ofNullable(backends);
            this.defaultBackends = ofNullable(defaultBackends);
        }

        public Builder backends(List<SuggestModule> backends) {
            this.backends = of(backends);
            return this;
        }

        public Builder defaultBackends(List<String> defaultBackends) {
            this.defaultBackends = of(defaultBackends);
            return this;
        }

        public Builder merge(Builder o) {
            // @formatter:off
            return new Builder(
                mergeOptionalList(o.backends, backends),
                mergeOptionalList(o.defaultBackends, defaultBackends)
            );
            // @formatter:on
        }

        public SuggestManagerModule build() {
            // @formatter:off
            return new SuggestManagerModule(
                backends.orElseGet(ImmutableList::of),
                defaultBackends
            );
            // @formatter:on
        }
    }
}
