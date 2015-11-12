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

package com.spotify.heroic.aggregationcache;

import static com.spotify.heroic.common.Optionals.pickOptional;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import java.util.Optional;

import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.HeroicReporter;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
public class AggregationCacheModule extends PrivateModule {
    private final AggregationCacheBackendModule backend;

    @Provides
    @Singleton
    public AggregationCacheReporter reporter(HeroicReporter reporter) {
        return reporter.newAggregationCache();
    }

    @Override
    protected void configure() {
        install(backend.module());
        bind(AggregationCache.class).to(AggregationCacheImpl.class).in(Scopes.SINGLETON);
        expose(AggregationCache.class);
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Optional<AggregationCacheBackendModule> backend = Optional.empty();

        @JsonCreator
        public Builder(@JsonProperty("backend") AggregationCacheBackendModule backend) {
            this.backend = ofNullable(backend);
        }

        public Builder backend(AggregationCacheBackendModule backend) {
            this.backend = of(backend);
            return this;
        }

        public Builder merge(final Builder o) {
            return new Builder(pickOptional(backend, o.backend));
        }

        public AggregationCacheModule build() {
            return new AggregationCacheModule(
                    backend.orElseGet(InMemoryAggregationCacheBackendConfig.builder()::build));
        }
    }
}
