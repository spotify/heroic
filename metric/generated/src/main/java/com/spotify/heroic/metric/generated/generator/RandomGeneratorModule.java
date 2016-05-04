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

package com.spotify.heroic.metric.generated.generator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.metric.generated.GeneratedComponent;
import com.spotify.heroic.metric.generated.GeneratorModule;
import com.spotify.heroic.metric.generated.GeneratorScope;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.Data;

import javax.inject.Named;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Data
public class RandomGeneratorModule implements GeneratorModule {
    private static final double DEFAULT_MIN = -100d;
    private static final double DEFAULT_MAX = 1000d;
    private static final long DEFAULT_STEP = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
    private static final double DEFAULT_RANGE = 50d;

    private final double min;
    private final double max;
    private final long step;
    private final double range;

    @JsonCreator
    public RandomGeneratorModule(
        @JsonProperty("min") Optional<Double> min, @JsonProperty("max") Optional<Double> max,
        @JsonProperty("step") Optional<Long> step, @JsonProperty("range") Optional<Double> range
    ) {
        this.min = min.orElse(DEFAULT_MIN);
        this.max = max.orElse(DEFAULT_MAX);
        this.step = step.orElse(DEFAULT_STEP);
        this.range = range.orElse(DEFAULT_RANGE);
    }

    @Override
    public GeneratedComponent module(PrimaryComponent primary) {
        return DaggerRandomGeneratorModule_C.builder().primaryComponent(primary).m(new M()).build();
    }

    @GeneratorScope
    @Component(modules = M.class, dependencies = PrimaryComponent.class)
    interface C extends GeneratedComponent {
        @Override
        RandomGenerator generator();
    }

    @Module
    class M {
        @Provides
        @Named("min")
        @GeneratorScope
        public double min() {
            return min;
        }

        @Provides
        @Named("max")
        @GeneratorScope
        public double max() {
            return max;
        }

        @Provides
        @Named("step")
        @GeneratorScope
        public long step() {
            return step;
        }

        @Provides
        @Named("range")
        @GeneratorScope
        public double range() {
            return range;
        }
    }
}
