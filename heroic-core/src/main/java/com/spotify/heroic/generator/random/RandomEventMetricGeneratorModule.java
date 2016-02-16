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

package com.spotify.heroic.generator.random;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.generator.MetricGeneratorModule;
import dagger.Component;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.empty;

public class RandomEventMetricGeneratorModule implements MetricGeneratorModule {
    public static final Duration DEFAULT_STEP = Duration.of(10, TimeUnit.MINUTES);

    private final Optional<String> id;
    private final Duration step;

    @JsonCreator
    public RandomEventMetricGeneratorModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("step") Optional<Duration> step
    ) {
        this.id = id;
        this.step = step.orElse(DEFAULT_STEP);
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public Exposed module(
        final PrimaryComponent primary, final Depends depends, final String id
    ) {
        return DaggerRandomEventMetricGeneratorModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M())
            .build();
    }

    @Override
    public String buildId(final int index) {
        return "random-events";
    }

    @RandomEventScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class, Depends.class})
    interface C extends Exposed {
        @Override
        RandomEventGenerator generator();
    }

    @Module
    class M {
        @Provides
        @Named("step")
        Duration step() {
            return step;
        }
    }

    public static RandomEventMetricGeneratorModule defaultInstance() {
        return new RandomEventMetricGeneratorModule(empty(), empty());
    }
}
