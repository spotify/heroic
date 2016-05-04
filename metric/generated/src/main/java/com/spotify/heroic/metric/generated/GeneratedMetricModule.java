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

package com.spotify.heroic.metric.generated;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.generated.generator.SineGeneratorModule;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.Data;

import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@Data
public final class GeneratedMetricModule implements MetricModule {
    public static final String DEFAULT_GROUP = "generated";

    private final Optional<String> id;
    private final Groups groups;
    private final GeneratorModule generatorModule;

    @JsonCreator
    public GeneratedMetricModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("generator") Optional<GeneratorModule> generatorModule
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.generatorModule = generatorModule.orElseGet(SineGeneratorModule::defaultSupplier);
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, String id) {
        final GeneratedComponent g = generatorModule.module(primary);

        return DaggerGeneratedMetricModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .generatedComponent(g)
            .m(new M())
            .build();
    }

    @GeneratedScope
    @Component(modules = M.class,
        dependencies = {PrimaryComponent.class, Depends.class, GeneratedComponent.class})
    interface C extends Exposed {
        @Override
        GeneratedBackend backend();
    }

    @Module
    class M {
        @Provides
        @GeneratedScope
        public Groups groups() {
            return groups;
        }
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("generated#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> id = empty();
        private Optional<Groups> groups = empty();
        private Optional<GeneratorModule> generatorModule = empty();

        public Builder id(String id) {
            this.id = of(id);
            return this;
        }

        public Builder groups(Groups groups) {
            this.groups = of(groups);
            return this;
        }

        public Builder generatorModule(GeneratorModule generatorModule) {
            this.generatorModule = of(generatorModule);
            return this;
        }

        public GeneratedMetricModule build() {
            return new GeneratedMetricModule(id, groups, generatorModule);
        }
    }
}
