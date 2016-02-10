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

package com.spotify.heroic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
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

import java.util.concurrent.ExecutorService;

import javax.inject.Named;
import javax.inject.Singleton;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.TinyAsync;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HeroicLoadingModule extends AbstractModule {
    private final ExecutorService executor;
    private final HeroicInternalLifeCycle lifeCycle;
    private final HeroicConfiguration options;
    private final HeroicReporterConfiguration reporterConfig;
    private final ExtraParameters parameters;

    @Provides
    @Singleton
    HeroicReporterConfiguration reporterConfig() {
        return reporterConfig;
    }

    @Provides
    @Singleton
    ExtraParameters parameters() {
        return parameters;
    }

    @Provides
    @Singleton
    HeroicConfiguration options() {
        return options;
    }

    @Provides
    @Singleton
    @Named("common")
    SerializerFramework serializer() {
        return TinySerializer.builder().build();
    }

    @Provides
    @Singleton
    FilterRegistry filterRegistry(@Named("common") SerializerFramework s) {
        return new CoreFilterRegistry(s, s.fixedInteger(), s.string());
    }

    @Provides
    @Singleton
    FilterSerializer filterSerializer(FilterRegistry filterRegistry) {
        return filterRegistry.newFilterSerializer();
    }

    @Provides
    @Singleton
    AggregationRegistry aggregationRegistry(@Named("common") SerializerFramework s) {
        return new CoreAggregationRegistry(s.string());
    }

    @Provides
    @Singleton
    public AggregationFactory aggregationFactory(AggregationRegistry configuration) {
        return configuration.newAggregationFactory();
    }

    @Provides
    @Singleton
    public AggregationSerializer aggregationSerializer(AggregationRegistry configuration) {
        return configuration.newAggregationSerializer();
    }

    @Provides
    @Singleton
    Serializer<Series> series(@Named("common") SerializerFramework s) {
        return new Series_Serializer(s);
    }

    @Provides
    @Singleton
    public AsyncFramework async(ExecutorService executor) {
        return TinyAsync.builder().executor(executor).build();
    }

    @Provides
    @Singleton
    @Named(HeroicCore.APPLICATION_HEROIC_CONFIG)
    private ObjectMapper configMapper() {
        return HeroicMappers.config();
    }

    @Override
    protected void configure() {
        bind(Scheduler.class).toInstance(new DefaultScheduler());
        bind(HeroicInternalLifeCycle.class).toInstance(lifeCycle);
        bind(FilterFactory.class).to(CoreFilterFactory.class).in(Scopes.SINGLETON);
        bind(FilterModifier.class).to(CoreFilterModifier.class).in(Scopes.SINGLETON);

        bind(HeroicConfigurationContext.class).to(CoreHeroicConfigurationContext.class)
                .in(Scopes.SINGLETON);

        bind(HeroicContext.class).toInstance(new CoreHeroicContext());
        bind(ExecutorService.class).toInstance(executor);

        bind(JavaxRestFramework.class).toInstance(new CoreJavaxRestFramework());
    }
}
