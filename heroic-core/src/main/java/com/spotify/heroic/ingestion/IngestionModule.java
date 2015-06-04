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

package com.spotify.heroic.ingestion;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.IngestionManagerReporter;

@RequiredArgsConstructor
public class IngestionModule extends PrivateModule {
    public static final boolean DEFAULT_UPDATE_METRICS = true;
    public static final boolean DEFAULT_UPDATE_METADATA = true;
    public static final boolean DEFAULT_UPDATE_SUGGESTIONS = true;

    private final boolean updateMetrics;
    private final boolean updateMetadata;
    private final boolean updateSuggestions;

    @JsonCreator
    public IngestionModule(@JsonProperty("updateMetrics") Boolean updateMetrics,
            @JsonProperty("updateMetadata") Boolean updateMetadata,
            @JsonProperty("updateSuggestions") Boolean updateSuggestions) {
        this.updateMetadata = Optional.fromNullable(updateMetadata).or(DEFAULT_UPDATE_METADATA);
        this.updateMetrics = Optional.fromNullable(updateMetrics).or(DEFAULT_UPDATE_METRICS);
        this.updateSuggestions = Optional.fromNullable(updateSuggestions).or(DEFAULT_UPDATE_SUGGESTIONS);
    }

    @Provides
    @Singleton
    public IngestionManagerReporter reporter(HeroicReporter reporter) {
        return reporter.newIngestionManager();
    }

    @Override
    protected void configure() {
        bind(IngestionManager.class).toInstance(
                new IngestionManagerImpl(updateMetrics, updateMetadata, updateSuggestions));
        expose(IngestionManager.class);
    }

    public static Supplier<IngestionModule> defaultSupplier() {
        return new Supplier<IngestionModule>() {
            @Override
            public IngestionModule get() {
                return new IngestionModule(null, null, null);
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Boolean updateMetrics;
        private Boolean updateMetadata;
        private Boolean updateSuggestions;

        public Builder updateAll() {
            this.updateMetrics = true;
            this.updateMetadata = true;
            this.updateSuggestions = true;
            return this;
        }

        public Builder updateMetrics(Boolean updateMetrics) {
            this.updateMetrics = updateMetrics;
            return this;
        }

        public Builder updateMetadata(Boolean updateMetadata) {
            this.updateMetadata = updateMetadata;
            return this;
        }

        public Builder updateSuggestions(Boolean updateSuggestions) {
            this.updateSuggestions = updateSuggestions;
            return this;
        }

        public IngestionModule build() {
            return new IngestionModule(updateMetrics, updateMetadata, updateSuggestions);
        }
    }
}
