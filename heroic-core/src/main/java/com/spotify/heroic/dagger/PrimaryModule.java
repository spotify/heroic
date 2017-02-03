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
import com.spotify.heroic.CoreHeroicContext;
import com.spotify.heroic.CoreQueryManager;
import com.spotify.heroic.CoreShellTasks;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.ShellTasks;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.grammar.CoreQueryParser;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.lifecycle.CoreLifeCycleManager;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.ShellTaskDefinition;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.statistics.HeroicReporter;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

@RequiredArgsConstructor
@Module
public class PrimaryModule {
    private final HeroicCoreInstance instance;
    private final FeatureSet features;
    private final HeroicReporter reporter;

    @Provides
    @PrimaryScope
    ShellTasks tasks(AsyncFramework async, HeroicCoreInstance injector) {
        final List<ShellTaskDefinition> commands = Tasks.available();

        try {
            return new CoreShellTasks(commands, setupTasks(commands, injector), async);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Provides
    @PrimaryScope
    HeroicCoreInstance instance() {
        return instance;
    }

    @Provides
    @PrimaryScope
    HeroicReporter reporter() {
        return reporter;
    }

    @Provides
    @PrimaryScope
    LifeCycleManager lifeCycleManager(CoreLifeCycleManager manager) {
        return manager;
    }

    @Provides
    @Named("features")
    @PrimaryScope
    Features features() {
        return Features.DEFAULT.applySet(features);
    }

    @Provides
    @Named(HeroicMappers.APPLICATION_JSON_INTERNAL)
    @PrimaryScope
    ObjectMapper internalMapper(
        QueryParser parser, AggregationRegistry aggregation
    ) {
        final ObjectMapper m = HeroicMappers.json(parser);

        /* configuration determined at runtime, unsuitable for testing */
        m.registerModule(aggregation.module());

        return m;
    }

    @Provides
    @Named(HeroicMappers.APPLICATION_JSON)
    @PrimaryScope
    ObjectMapper jsonMapper(@Named(HeroicMappers.APPLICATION_JSON_INTERNAL) ObjectMapper mapper) {
        return mapper;
    }

    @Provides
    @PrimaryScope
    QueryParser queryParser(CoreQueryParser queryParser) {
        return queryParser;
    }

    @Provides
    @PrimaryScope
    QueryManager queryManager(CoreQueryManager queryManager) {
        return queryManager;
    }

    @Provides
    @PrimaryScope
    HeroicContext context(CoreHeroicContext context) {
        return context;
    }

    private SortedMap<String, ShellTask> setupTasks(
        final List<ShellTaskDefinition> commands, final HeroicCoreInstance injector
    ) throws Exception {
        final SortedMap<String, ShellTask> tasks = new TreeMap<>();

        for (final ShellTaskDefinition def : commands) {
            final ShellTask instance = def.setup(injector);

            for (final String n : def.names()) {
                tasks.put(n, instance);
            }
        }

        return tasks;
    }
}
