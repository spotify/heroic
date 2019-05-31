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

package com.spotify.heroic.statistics.semantic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.spotify.heroic.common.ServiceInfo;
import com.spotify.heroic.dagger.EarlyComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.StatisticsComponent;
import com.spotify.heroic.statistics.StatisticsModule;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.ffwd.FastForwardReporter;
import com.spotify.metrics.jvm.CpuGaugeSet;
import com.spotify.metrics.jvm.GarbageCollectorMetricSet;
import com.spotify.metrics.jvm.MemoryUsageGaugeSet;
import com.spotify.metrics.jvm.ThreadStatesMetricSet;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Module
public class SemanticStatisticsModule implements StatisticsModule {
    @JsonCreator
    public SemanticStatisticsModule() {
    }

    @Override
    public StatisticsComponent module(final EarlyComponent early) {
        return DaggerSemanticStatisticsComponent
            .builder()
            .earlyComponent(early)
            .semanticStatisticsModule(this)
            .build();
    }

    @Provides
    @SemanticStatisticsScope
    public SemanticMetricRegistry registry() {
        return new SemanticMetricRegistry();
    }

    @Provides
    @SemanticStatisticsScope
    public LifeCycle life(
        AsyncFramework async, SemanticMetricRegistry registry, ServiceInfo info,
        LifeCycleRegistry lifeCycleRegistry
    ) {
        final MetricId gauges = MetricId.build();

        registry.register(gauges, new ThreadStatesMetricSet());
        registry.register(gauges, new GarbageCollectorMetricSet());
        registry.register(gauges, new MemoryUsageGaugeSet());
        registry.register(gauges, CpuGaugeSet.create());

        final MetricId metric =
            MetricId.build("heroic").tagged("service", "heroic", "heroic_id", info.getId());

        final FastForwardReporter reporter;

        try {
            reporter = FastForwardReporter
                .forRegistry(registry)
                .schedule(TimeUnit.SECONDS, 30)
                .prefix(metric)
                .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to configure reporter", e);
        }

        return () -> {
            final LifeCycleRegistry scope = lifeCycleRegistry.scoped("ffwd-reporter");

            scope.start(() -> async.call(() -> {
                reporter.start();
                return null;
            }));

            scope.stop(() -> async.call(() -> {
                reporter.stop();
                return null;
            }));
        };
    }

    @Provides
    @SemanticStatisticsScope
    public HeroicReporter reporter(SemanticMetricRegistry registry) {
        return new SemanticHeroicReporter(registry);
    }
}
