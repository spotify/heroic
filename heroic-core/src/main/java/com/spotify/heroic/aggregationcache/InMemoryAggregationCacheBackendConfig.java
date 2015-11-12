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

import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Supplier;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;

public class InMemoryAggregationCacheBackendConfig implements AggregationCacheBackendModule {
    @JsonCreator
    public static Supplier<InMemoryAggregationCacheBackendConfig> defaultSupplier() {
        return new Supplier<InMemoryAggregationCacheBackendConfig>() {
            @Override
            public InMemoryAggregationCacheBackendConfig get() {
                return new InMemoryAggregationCacheBackendConfig();
            }
        };
    }

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            public AggregationCacheBackendReporter reporter(AggregationCacheReporter reporter) {
                return reporter.newAggregationCacheBackend();
            }

            @Override
            protected void configure() {
                bind(AggregationCacheBackend.class).to(InMemoryAggregationCacheBackend.class)
                        .in(Scopes.SINGLETON);
                expose(AggregationCacheBackend.class);
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public AggregationCacheBackendModule build() {
            return new InMemoryAggregationCacheBackendConfig();
        }
    }
}
