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

import java.util.concurrent.TimeUnit;

import javax.inject.Named;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.metric.generated.Generator;
import com.spotify.heroic.metric.generated.GeneratorModule;

@Data
public class SineGeneratorModule implements GeneratorModule {
    private static final double DEFAULT_MAGNITUDE = 1000d;
    private static final long DEFAULT_PERIOD = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    private static final long DEFAULT_STEP = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

    private final double magnitude;
    private final long period;
    private final long step;

    @JsonCreator
    public SineGeneratorModule(@JsonProperty("magnitude") Double magnitude,
            @JsonProperty("period") Long period, @JsonProperty("step") Long step) {
        this.magnitude = Optional.fromNullable(magnitude).or(DEFAULT_MAGNITUDE);
        this.period = Optional.fromNullable(period).or(DEFAULT_PERIOD);
        this.step = Optional.fromNullable(step).or(DEFAULT_STEP);
    }

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Named("magnitude")
            public double magnitude() {
                return magnitude;
            }

            @Provides
            @Named("period")
            public long period() {
                return period;
            }

            @Provides
            @Named("step")
            public long frequency() {
                return step;
            }

            @Override
            protected void configure() {
                bind(Generator.class).to(SineGenerator.class).in(Scopes.SINGLETON);
                expose(Generator.class);
            }
        };
    }

    public static Supplier<GeneratorModule> defaultSupplier() {
        return new Supplier<GeneratorModule>() {
            @Override
            public GeneratorModule get() {
                return new SineGeneratorModule(null, null, null);
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Double magnitude;
        private Long period;
        private Long step;

        public Builder magnitude(Double magnitude) {
            this.magnitude = magnitude;
            return this;
        }

        public Builder period(Long period) {
            this.period = period;
            return this;
        }

        public Builder step(Long step) {
            this.step = step;
            return this;
        }

        public SineGeneratorModule build() {
            return new SineGeneratorModule(magnitude, period, step);
        }
    }
}
