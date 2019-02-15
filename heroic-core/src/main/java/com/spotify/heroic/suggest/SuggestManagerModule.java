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

import static com.spotify.heroic.common.Optionals.mergeOptionalList;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.GroupSet;
import com.spotify.heroic.common.ModuleIdBuilder;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.SuggestBackendReporter;
import dagger.Module;
import dagger.Provides;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.inject.Named;

@Module
public class SuggestManagerModule {
    private final List<SuggestModule> backends;
    private final Optional<List<String>> defaultBackends;

    public SuggestManagerModule(
        List<SuggestModule> backends,
        Optional<List<String>> defaultBackends
    ) {
        this.backends = backends;
        this.defaultBackends = defaultBackends;
    }

    @Provides
    @SuggestScope
    public SuggestBackendReporter localReporter(HeroicReporter reporter) {
        return reporter.newSuggestBackend();
    }

    @Provides
    @Named("groupSet")
    @SuggestScope
    public GroupSet<SuggestBackend> groupSet(Set<SuggestBackend> configured) {
        return GroupSet.build(configured, defaultBackends);
    }

    @Provides
    @SuggestScope
    public List<SuggestModule.Exposed> components(
        SuggestBackendReporter reporter, PrimaryComponent primary
    ) {
        final ArrayList<SuggestModule.Exposed> results = new ArrayList<>();

        final ModuleIdBuilder idBuilder = new ModuleIdBuilder();

        for (final SuggestModule m : backends) {
            final String id = idBuilder.buildId(m);

            final SuggestModule.Depends depends = new SuggestModule.Depends(reporter);
            results.add(m.module(primary, depends, id));
        }

        return results;
    }

    @Provides
    @SuggestScope
    public Set<SuggestBackend> backends(
        List<SuggestModule.Exposed> components, SuggestBackendReporter reporter
    ) {
        return ImmutableSet.copyOf(components
            .stream()
            .map(SuggestModule.Exposed::backend)
            .map(reporter::decorate)
            .iterator());
    }

    @Provides
    @SuggestScope
    @Named("suggest")
    public LifeCycle suggestLife(List<SuggestModule.Exposed> components) {
        return LifeCycle.combined(components.stream().map(SuggestModule.Exposed::life));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<List<SuggestModule>> backends = empty();
        private Optional<List<String>> defaultBackends = empty();

        private Builder() {
        }

        @JsonCreator
        public Builder(
            @JsonProperty("backends") Optional<List<SuggestModule>> backends,
            @JsonProperty("defaultBackends") Optional<List<String>> defaultBackends
        ) {
            this.backends = backends;
            this.defaultBackends = defaultBackends;
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
