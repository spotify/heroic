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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.heroic.CoreHeroicConfigurationContext;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfiguration;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.aggregation.CoreAggregationRegistry;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Series_Serializer;
import com.spotify.heroic.filter.CoreFilterModifier;
import com.spotify.heroic.filter.FilterModifier;
import com.spotify.heroic.lifecycle.CoreLifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.scheduler.DefaultScheduler;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.time.Clock;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.TinyAsync;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.inject.Named;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Module
public class LoadingModule {
    private final ExecutorService executor;
    /* {@code true} if this instance manages its own executor and is responsible for stopping it */
    private final boolean managedExecutor;
    private final HeroicConfiguration options;
    private final ExtraParameters parameters;

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
    Serializer<Series> series(@Named("common") SerializerFramework s) {
        return new Series_Serializer(s);
    }

    @Provides
    @LoadingScope
    AsyncFramework async(ExecutorService executor, ScheduledExecutorService scheduler) {
        return TinyAsync
            .builder()
            .recursionSafe(true)
            .executor(executor)
            .scheduler(scheduler)
            .build();
    }

    @Provides
    @Named(HeroicMappers.APPLICATION_HEROIC_CONFIG)
    @LoadingScope
    ObjectMapper configMapper() {
        return HeroicMappers.config();
    }

    @Provides
    @LoadingScope
    ScheduledExecutorService scheduledExecutorService() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10,
            new ThreadFactoryBuilder().setNameFormat("heroic-scheduler#%d").build());
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }

    @Provides
    @LoadingScope
    Scheduler scheduler(final ScheduledExecutorService scheduler) {
        return new DefaultScheduler(scheduler);
    }

    @Provides
    @LoadingScope
    FilterModifier coreFilterModifier() {
        return new CoreFilterModifier();
    }

    @Provides
    @LoadingScope
    ExecutorService executorService() {
        return executor;
    }

    @Provides
    @LoadingScope
    HeroicConfigurationContext heroicConfigurationContext(
        @Named("application/heroic-config") final ObjectMapper mapper
    ) {
        return new CoreHeroicConfigurationContext(mapper);
    }

    @Provides
    @LoadingScope
    @Named("internal")
    LifeCycleRegistry internalLifeCycleRegistry() {
        return new CoreLifeCycleRegistry();
    }

    @Provides
    @LoadingScope
    @Named("loading")
    LifeCycle loadingLifeCycles(
        @Named("internal") LifeCycleRegistry registry, final AsyncFramework async,
        final ScheduledExecutorService scheduler, final ExecutorService executor
    ) {
        return () -> {
            registry.scoped("loading scheduler").stop(() -> async.call(() -> {
                shutdown(scheduler);
                return null;
            }, ForkJoinPool.commonPool()));

            if (managedExecutor) {
                registry.scoped("loading executor").stop(() -> async.call(() -> {
                    shutdown(executor);
                    return null;
                }, ForkJoinPool.commonPool()));
            }
        };
    }

    private void shutdown(final ExecutorService executor) {
        executor.shutdown();

        final boolean terminated;

        try {
            terminated = executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (!terminated) {
            throw new RuntimeException("executor did not shut down in time");
        }
    }

    @Provides
    @LoadingScope
    Clock clock() {
        return Clock.system();
    }
}
