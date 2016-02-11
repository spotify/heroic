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

package com.spotify.heroic.consumer;

import com.spotify.heroic.consumer.ConsumerModule.Out;
import com.spotify.heroic.dagger.CorePrimaryComponent;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.statistics.HeroicReporter;
import dagger.Module;
import dagger.Provides;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor
@Module
public class CoreConsumersModule {
    private final HeroicReporter reporter;
    private final List<ConsumerModule> consumers;
    private final CorePrimaryComponent primary;
    private final IngestionComponent ingestion;

    @Provides
    @ConsumersScope
    List<ConsumerModule.Out> components() {
        final List<ConsumerModule.Out> consumers = new ArrayList<>();

        final AtomicInteger i = new AtomicInteger();

        for (final ConsumerModule m : this.consumers) {
            final String id = m.id().orElseGet(() -> m.buildId(i.getAndIncrement()));

            final ConsumerModule.In in = new ConsumerModule.In(reporter.newConsumer(id));

            consumers.add(m.module(primary, ingestion, in, id));
        }

        return consumers;
    }

    @Provides
    @ConsumersScope
    Set<Consumer> consumers(List<ConsumerModule.Out> components) {
        final Set<Consumer> consumers = new HashSet<>();

        for (final ConsumerModule.Out m : components) {
            consumers.add(m.consumer());
        }

        return consumers;
    }

    @Provides
    @ConsumersScope
    @Named("consumers")
    LifeCycle consumersLife(List<ConsumerModule.Out> components) {
        return LifeCycle.combined(components.stream().map(Out::consumerLife));
    }
}
