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
import com.spotify.heroic.metadata.MetadataModule.Exposed;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import dagger.Module;
import dagger.Provides;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.inject.Named;

@Module
public class MetadataManagerModule {
    private final List<MetadataModule> backends;
    private final Optional<List<String>> defaultBackends;

    public MetadataManagerModule(
        List<MetadataModule> backends,
        Optional<List<String>> defaultBackends
    ) {
        this.backends = backends;
        this.defaultBackends = defaultBackends;
    }

    @Provides
    @MetadataScope
    public MetadataBackendReporter localReporter(HeroicReporter reporter) {
        return reporter.newMetadataBackend();
    }

    @Provides
    @MetadataScope
    public List<Exposed> components(
        final PrimaryComponent primary, final MetadataBackendReporter reporter
    ) {
        final List<Exposed> results = new ArrayList<>();

        final ModuleIdBuilder idBuilder = new ModuleIdBuilder();

        for (final MetadataModule m : backends) {
            final String id = idBuilder.buildId(m);
            final MetadataModule.Depends depends = new MetadataModule.Depends(reporter);
            results.add(m.module(primary, depends, id));
        }

        return results;
    }

    @Provides
    @MetadataScope
    public Set<MetadataBackend> backends(
        List<Exposed> components, MetadataBackendReporter reporter
    ) {
        return ImmutableSet.copyOf(
            components.stream().map(Exposed::backend).map(reporter::decorate).iterator());
    }

    @Provides
    @Named("groupSet")
    @MetadataScope
    public GroupSet<MetadataBackend> groupSet(Set<MetadataBackend> configured) {
        return GroupSet.build(configured, defaultBackends);
    }

    @Provides
    @Named("metadata")
    @MetadataScope
    LifeCycle metadataLife(List<Exposed> components) {
        return LifeCycle.combined(components.stream().map(c -> c.life()));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<List<MetadataModule>> backends = empty();
        private Optional<List<String>> defaultBackends = empty();

        private Builder() {
        }

        @JsonCreator
        public Builder(
            @JsonProperty("backends") Optional<List<MetadataModule>> backends,
            @JsonProperty("defaultBackends") Optional<List<String>> defaultBackends
        ) {
            this.backends = backends;
            this.defaultBackends = defaultBackends;
        }

        public Builder backends(List<MetadataModule> backends) {
            this.backends = of(backends);
            return this;
        }

        public Builder defaultBackends(List<String> defaultBackends) {
            this.defaultBackends = of(defaultBackends);
            return this;
        }

        public Builder merge(final Builder o) {
            // @formatter:off
            return new Builder(
                mergeOptionalList(o.backends, backends),
                mergeOptionalList(o.defaultBackends, defaultBackends)
            );
            // @formatter:on
        }

        public MetadataManagerModule build() {
            // @formatter:off
            return new MetadataManagerModule(
                backends.orElseGet(ImmutableList::of),
                defaultBackends
            );
            // @formatter:on
        }
    }
}
