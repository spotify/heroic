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

package com.spotify.heroic.generator.sine;

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

public class SineMetricGeneratorModule implements MetricGeneratorModule {
    public static final double DEFAULT_MAGNITUDE = 100D;
    public static final Duration DEFAULT_PERIOD = Duration.of(1, TimeUnit.DAYS);
    public static final double DEFAULT_JITTER = 50D;
    public static final double DEFAULT_OFFSET = 1000D;

    private final Optional<String> id;
    private final double magnitude;
    private final Duration period;
    private final Duration step;
    private final double jitter;
    private final double offset;

    @JsonCreator
    public SineMetricGeneratorModule(
        @JsonProperty("id") Optional<String> id,
        @JsonProperty("magnitude") Optional<Double> magnitude,
        @JsonProperty("period") Optional<Duration> period,
        @JsonProperty("step") Optional<Duration> step,
        @JsonProperty("jitter") Optional<Double> jitter,
        @JsonProperty("offset") Optional<Double> offset
    ) {
        this.id = id;
        this.magnitude = magnitude.orElse(DEFAULT_MAGNITUDE);
        this.period = period.orElse(DEFAULT_PERIOD);
        this.step = step.orElseGet(
            () -> Duration.of(this.period.toMilliseconds() / 100, TimeUnit.MILLISECONDS));
        this.jitter = jitter.orElse(DEFAULT_JITTER);
        this.offset = offset.orElse(DEFAULT_OFFSET);
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public Exposed module(
        final PrimaryComponent primary, final Depends depends, final String id
    ) {
        return DaggerSineMetricGeneratorModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M())
            .build();
    }

    @Override
    public String buildId(final int index) {
        return "sine";
    }

    @SineScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class, Depends.class})
    interface C extends Exposed {
        @Override
        SineGenerator generator();
    }

    @Module
    class M {
        @Provides
        @Named("magnitude")
        double magnitude() {
            return magnitude;
        }

        @Provides
        @Named("period")
        Duration period() {
            return period;
        }

        @Provides
        @Named("step")
        Duration step() {
            return step;
        }

        @Provides
        @Named("jitter")
        double jitter() {
            return jitter;
        }

        @Provides
        @Named("offset")
        double offset() {
            return offset;
        }
    }

    public static SineMetricGeneratorModule defaultInstance() {
        return new SineMetricGeneratorModule(empty(), empty(), empty(), empty(), empty(), empty());
    }
}
