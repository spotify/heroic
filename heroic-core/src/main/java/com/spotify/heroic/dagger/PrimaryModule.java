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
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.HeroicServer;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.ShellTasks;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.common.ServiceInfo;
import com.spotify.heroic.filter.FilterRegistry;
import com.spotify.heroic.grammar.CoreQueryParser;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.jetty.JettyServerConnector;
import com.spotify.heroic.lifecycle.CoreLifeCycleManager;
import com.spotify.heroic.lifecycle.CoreLifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.ShellTaskDefinition;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.statistics.HeroicReporter;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

@RequiredArgsConstructor
@Module
public class PrimaryModule {
    private final Optional<String> id;
    private final HeroicCoreInstance instance;
    private final InetSocketAddress bindAddress;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;
    private final Set<String> features;
    private final List<JettyServerConnector> connectors;

    private final HeroicReporter reporter;

    private final String service;
    private final String version;

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
    LifeCycleRegistry lifeCycleRegistry(CoreLifeCycleRegistry registry) {
        return registry;
    }

    @Provides
    @PrimaryScope
    LifeCycleManager lifeCycleManager(CoreLifeCycleManager manager) {
        return manager;
    }

    @Provides
    @Named("bindAddress")
    @PrimaryScope
    InetSocketAddress bindAddress() {
        return bindAddress;
    }

    @Provides
    @Named("enableCors")
    @PrimaryScope
    boolean enableCors() {
        return enableCors;
    }

    @Provides
    @Named("corsAllowOrigin")
    @PrimaryScope
    Optional<String> corsAllowOrigin() {
        return corsAllowOrigin;
    }

    @Provides
    @Named("features")
    @PrimaryScope
    Set<String> features() {
        return features;
    }

    @Provides
    @PrimaryScope
    ServiceInfo service() {
        return new ServiceInfo(service, version, id.orElse("heroic"));
    }

    @Provides
    @PrimaryScope
    List<JettyServerConnector> connectors() {
        return connectors;
    }

    @Provides
    @Named(HeroicCore.APPLICATION_JSON_INTERNAL)
    @PrimaryScope
    ObjectMapper internalMapper(
        FilterRegistry filterRegistry, QueryParser parser, AggregationRegistry aggregation
    ) {
        final ObjectMapper m = HeroicMappers.json();

        /* configuration determined at runtime, unsuitable for testing */
        m.registerModule(filterRegistry.module(parser));
        m.registerModule(aggregation.module());

        return m;
    }

    @Provides
    @Named(HeroicCore.APPLICATION_JSON)
    @PrimaryScope
    ObjectMapper jsonMapper(@Named(HeroicCore.APPLICATION_JSON_INTERNAL) ObjectMapper mapper) {
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

    @Provides
    @PrimaryScope
    @Named("heroicServer")
    LifeCycle heroicServerLife(LifeCycleManager manager, HeroicServer server) {
        return manager.build(server);
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
