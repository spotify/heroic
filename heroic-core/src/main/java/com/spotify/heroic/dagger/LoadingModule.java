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

package com.spotify.heroic.dagger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.CoreHeroicConfigurationContext;
import com.spotify.heroic.CoreHeroicLifeCycle;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfiguration;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicLifeCycle;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.HeroicReporterConfiguration;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.CoreAggregationRegistry;
import com.spotify.heroic.common.CoreJavaxRestFramework;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Series_Serializer;
import com.spotify.heroic.filter.CoreFilterFactory;
import com.spotify.heroic.filter.CoreFilterModifier;
import com.spotify.heroic.filter.CoreFilterRegistry;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.filter.FilterModifier;
import com.spotify.heroic.filter.FilterRegistry;
import com.spotify.heroic.filter.FilterSerializer;
import com.spotify.heroic.scheduler.DefaultScheduler;
import com.spotify.heroic.scheduler.Scheduler;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.TinyAsync;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.util.concurrent.ExecutorService;

@RequiredArgsConstructor
@Module
public class LoadingModule {
    private final ExecutorService executor;
    private final HeroicConfiguration options;
    private final HeroicReporterConfiguration reporterConfig;
    private final ExtraParameters parameters;

    @Provides
    @LoadingScope
    HeroicReporterConfiguration reporterConfig() {
        return reporterConfig;
    }

    @Provides
    @LoadingScope
    ExtraParameters parameters() {
        return parameters;
    }

    @Provides
    @LoadingScope
    HeroicConfiguration options() {
        return options;
    }

    @Provides
    @Named("common")
    @LoadingScope
    SerializerFramework serializer() {
        return TinySerializer.builder().build();
    }

    @Provides
    @LoadingScope
    FilterRegistry filterRegistry(@Named("common") SerializerFramework s) {
        return new CoreFilterRegistry(s, s.fixedInteger(), s.string());
    }

    @Provides
    @LoadingScope
    FilterSerializer filterSerializer(FilterRegistry filterRegistry) {
        return filterRegistry.newFilterSerializer();
    }

    @Provides
    @LoadingScope
    AggregationRegistry aggregationRegistry(@Named("common") SerializerFramework s) {
        return new CoreAggregationRegistry(s.string());
    }

    @Provides
    @LoadingScope
    AggregationFactory aggregationFactory(AggregationRegistry configuration) {
        return configuration.newAggregationFactory();
    }

    @Provides
    @LoadingScope
    AggregationSerializer aggregationSerializer(AggregationRegistry configuration) {
        return configuration.newAggregationSerializer();
    }

    @Provides
    @LoadingScope
    Serializer<Series> series(@Named("common") SerializerFramework s) {
        return new Series_Serializer(s);
    }

    @Provides
    @LoadingScope
    AsyncFramework async(ExecutorService executor) {
        return TinyAsync.builder().executor(executor).build();
    }

    @Provides
    @Named(HeroicCore.APPLICATION_HEROIC_CONFIG)
    @LoadingScope
    ObjectMapper configMapper() {
        return HeroicMappers.config();
    }

    @Provides
    @LoadingScope
    Scheduler scheduler() {
        return new DefaultScheduler();
    }

    @Provides
    @LoadingScope
    HeroicLifeCycle heroicLifeCycle(CoreHeroicLifeCycle heroicLifeCycle) {
        return heroicLifeCycle;
    }

    @Provides
    @LoadingScope
    FilterFactory coreFilterFactory() {
        return new CoreFilterFactory();
    }

    @Provides
    @LoadingScope
    FilterModifier coreFilterModifier(final FilterFactory factory) {
        return new CoreFilterModifier(factory);
    }

    @Provides
    @LoadingScope
    ExecutorService executorService() {
        return executor;
    }

    @Provides
    @LoadingScope
    JavaxRestFramework javaxRestFramework() {
        return new CoreJavaxRestFramework();
    }

    @Provides
    @LoadingScope
    HeroicConfigurationContext heroicConfigurationContext(
        @Named("application/heroic-config") final ObjectMapper mapper
    ) {
        return new CoreHeroicConfigurationContext(mapper);
    }
}
