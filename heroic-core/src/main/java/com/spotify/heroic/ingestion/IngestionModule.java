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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.MetadataComponent;
import com.spotify.heroic.metric.MetricComponent;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.suggest.SuggestComponent;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.util.Optional;

import static com.spotify.heroic.common.Optionals.pickOptional;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

@RequiredArgsConstructor
public class IngestionModule {
    private static final String INGESTION_FILTER_PARAM = "ingestion.filter";

    public static final boolean DEFAULT_UPDATE_METRICS = true;
    public static final boolean DEFAULT_UPDATE_METADATA = true;
    public static final boolean DEFAULT_UPDATE_SUGGESTIONS = true;
    public static final int DEFAULT_MAX_CONCURRENT_WRITES = 50000;

    private final boolean updateMetrics;
    private final boolean updateMetadata;
    private final boolean updateSuggestions;
    private final int maxConcurrentWrites;
    private final Optional<String> filter;

    public IngestionComponent module(
        PrimaryComponent primary, SuggestComponent suggest, MetadataComponent metadata,
        MetricComponent metric
    ) {
        return DaggerIngestionModule_C
            .builder()
            .primaryComponent(primary)
            .suggestComponent(suggest)
            .metadataComponent(metadata)
            .metricComponent(metric)
            .m(new M())
            .build();
    }

    @IngestionScope
    @Component(modules = M.class,
        dependencies = {
            PrimaryComponent.class, SuggestComponent.class, MetadataComponent.class,
            MetricComponent.class
        })
    interface C extends IngestionComponent {
        @Override
        IngestionManagerImpl ingestionManager();
    }

    @Module
    class M {
        @Provides
        @IngestionScope
        public IngestionManagerReporter reporter(HeroicReporter reporter) {
            return reporter.newIngestionManager();
        }

        @Provides
        @Named("updateMetadata")
        @IngestionScope
        public boolean updateMetadata() {
            return updateMetadata;
        }

        @Provides
        @Named("updateMetrics")
        @IngestionScope
        public boolean updateMetrics() {
            return updateMetrics;
        }

        @Provides
        @Named("updateSuggestions")
        @IngestionScope
        public boolean updateSuggestions() {
            return updateSuggestions;
        }

        @Provides
        @Named("maxConcurrentWrites")
        @IngestionScope
        public int maxConcurrentWrites() {
            return maxConcurrentWrites;
        }

        @Provides
        @IngestionScope
        public Filter filter(
            final QueryParser parser, final FilterFactory filters, final ExtraParameters params
        ) {
            return Optionals
                .pickOptional(filter.map(parser::parseFilter),
                    params.getFilter(INGESTION_FILTER_PARAM, parser))
                .orElseGet(filters::t);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Optional<Boolean> updateMetrics = empty();
        private Optional<Boolean> updateMetadata = empty();
        private Optional<Boolean> updateSuggestions = empty();
        private Optional<Integer> maxConcurrentWrites = empty();
        private Optional<String> filter = empty();

        @JsonCreator
        public Builder(
            @JsonProperty("updateMetrics") Boolean updateMetrics,
            @JsonProperty("updateMetadata") Boolean updateMetadata,
            @JsonProperty("updateSuggestions") Boolean updateSuggestions,
            @JsonProperty("maxConcurrentWrites") Integer maxConcurrentWrites,
            @JsonProperty("filter") String filter
        ) {
            this.updateMetadata = ofNullable(updateMetadata);
            this.updateMetrics = ofNullable(updateMetrics);
            this.updateSuggestions = ofNullable(updateSuggestions);
            this.maxConcurrentWrites = ofNullable(maxConcurrentWrites);
            this.filter = ofNullable(filter);
        }

        public Builder updateAll() {
            this.updateMetrics = of(true);
            this.updateMetadata = of(true);
            this.updateSuggestions = of(true);
            return this;
        }

        public Builder updateMetrics(boolean updateMetrics) {
            this.updateMetrics = of(updateMetrics);
            return this;
        }

        public Builder updateMetadata(boolean updateMetadata) {
            this.updateMetadata = of(updateMetadata);
            return this;
        }

        public Builder updateSuggestions(boolean updateSuggestions) {
            this.updateSuggestions = of(updateSuggestions);
            return this;
        }

        public Builder maxConcurrentWrites(int maxConcurrentWrites) {
            this.maxConcurrentWrites = of(maxConcurrentWrites);
            return this;
        }

        public Builder merge(final Builder o) {
            // @formatter:off
            return new Builder(
                pickOptional(updateMetrics, o.updateMetrics),
                pickOptional(updateMetadata, o.updateMetadata),
                pickOptional(updateSuggestions, o.updateSuggestions),
                pickOptional(maxConcurrentWrites, o.maxConcurrentWrites),
                pickOptional(filter, o.filter)
            );
            // @formatter:on
        }

        public IngestionModule build() {
            // @formatter:off
            return new IngestionModule(
                updateMetrics.orElse(DEFAULT_UPDATE_METRICS),
                updateMetadata.orElse(DEFAULT_UPDATE_METADATA),
                updateSuggestions.orElse(DEFAULT_UPDATE_SUGGESTIONS),
                maxConcurrentWrites.orElse(DEFAULT_MAX_CONCURRENT_WRITES),
                filter
            );
            // @formatter:on
        }
    }
}
