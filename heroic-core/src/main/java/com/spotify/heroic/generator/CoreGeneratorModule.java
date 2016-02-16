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

package com.spotify.heroic.generator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.generator.random.RandomEventMetricGeneratorModule;
import com.spotify.heroic.generator.sine.SineMetricGeneratorModule;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.spotify.heroic.common.Optionals.mergeOptionalList;
import static com.spotify.heroic.common.Optionals.pickOptional;
import static java.util.Optional.empty;

@RequiredArgsConstructor
@Module
public class CoreGeneratorModule {
    private final List<MetricGeneratorModule> modules;
    private final MetadataGenerator metadata;

    public GeneratorComponent module(final PrimaryComponent primary) {
        return DaggerCoreGeneratorModule_C.builder().m(new M(primary)).build();
    }

    @GeneratorScope
    @Component(modules = M.class)
    interface C extends GeneratorComponent {
        @Override
        CoreGeneratorManager generatorManager();
    }

    @RequiredArgsConstructor
    @Module
    class M {
        private final PrimaryComponent primary;

        @Provides
        @GeneratorScope
        Map<String, Generator> generators() {
            final ImmutableMap.Builder<String, Generator> generators = ImmutableMap.builder();

            final AtomicInteger i = new AtomicInteger();

            for (final MetricGeneratorModule m : modules) {
                final String id = m.id().orElseGet(() -> m.buildId(i.getAndIncrement()));
                final MetricGeneratorModule.Depends depends = new MetricGeneratorModule.Depends();
                final MetricGeneratorModule.Exposed exposed = m.module(primary, depends, id);
                generators.put(id, exposed.generator());
            }

            return generators.build();
        }

        @Provides
        @GeneratorScope
        MetadataGenerator metadataGenerator() {
            return metadata;
        }
    }

    public static List<MetricGeneratorModule> defaultMetrics() {
        return ImmutableList.of(SineMetricGeneratorModule.defaultInstance(),
            RandomEventMetricGeneratorModule.defaultInstance());
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor
    public static class Builder {
        private Optional<List<MetricGeneratorModule>> metrics = empty();
        private Optional<MetadataGenerator> metadata = empty();

        @JsonCreator
        public Builder(
            @JsonProperty("metrics") Optional<List<MetricGeneratorModule>> metrics,
            @JsonProperty("metadata") Optional<MetadataGenerator> metadata
        ) {
            this.metrics = metrics;
            this.metadata = metadata;
        }

        public Builder merge(Builder o) {
            return new Builder(mergeOptionalList(metrics, o.metrics),
                pickOptional(metadata, o.metadata));
        }

        public CoreGeneratorModule build() {
            return new CoreGeneratorModule(metrics.orElseGet(CoreGeneratorModule::defaultMetrics),
                metadata.orElseGet(RandomMetadataGenerator::buildDefault));
        }
    }
}
